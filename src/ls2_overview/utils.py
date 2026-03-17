import math
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

import dask.array as da
import ngff_zarr as nz
import polars as pl

DIM_ORDER = ("t", "c", "z", "y", "x")
PHYSICAL_DIMS = ("t", "z", "y", "x")

THUMBNAIL_PATTERN = re.compile(
    r"(?P<position>[^_]+)_(?P<stack>[^_]+)\/(?P<view>[^_.]+)(\.ome)?(\.zarr)?(\.ozx)?"
)


def _check_dim_order(img: "nz.NgffImage"):
    order = [DIM_ORDER.index(dim) for dim in img.dims]
    if order != sorted(order):
        raise ValueError(f"Invalid dimension order: {img.dims} require: {DIM_ORDER}")


def to_napari(img: "nz.NgffImage", channel_ids: tuple[int] = (), **kwargs):
    _check_dim_order(img)

    if channel_ids == ():
        data = img.data
    else:
        assert "c" in img.dims, "Channel selection only valid if 'c' dim in image"

        slc = tuple(
            [slice(None) if dim != "c" else list(channel_ids) for dim in img.dims]
        )
        data = img.data[slc]

    return {
        "data": data.compute(),
        "scale": [v for k, v in img.scale.items() if k in PHYSICAL_DIMS],
        "translate": [v for k, v in img.translation.items() if k in PHYSICAL_DIMS],
        "channel_axis": None if "c" not in img.dims else img.dims.index("c"),
        **kwargs,
    }


def load_thumbnail(fn: str | Path) -> "nz.NgffImage":
    fn_local = Path(fn).relative_to(Path(fn).parent.parent)
    mo = THUMBNAIL_PATTERN.match(fn_local.as_posix())
    if mo is None:
        raise ValueError(f"Invalid filename {fn_local}")
    img = nz.from_ngff_zarr(fn).images[0]
    img.name = mo["position"]
    return img


def to_bbx_polygon(
    img: "nz.NgffImage", translate_y: int = 0, translate_x: int = 0
) -> dict[str, Any]:
    x_extent = img.data.shape[img.dims.index("x")] * img.scale["x"]
    y_extent = img.data.shape[img.dims.index("y")] * img.scale["y"]

    x_lower = img.translation["x"] - img.scale["x"] / 2 + translate_x
    y_lower = img.translation["y"] - img.scale["y"] / 2 + translate_y
    x_upper = x_lower + x_extent
    y_upper = y_lower + y_extent

    return _bbx_to_polygon(y_lower, x_lower, y_upper, x_upper)


def _bbx_to_polygon(y_lower, x_lower, y_upper, x_upper):
    return [
        [y_lower, x_lower],
        [y_lower, x_upper],
        [y_upper, x_upper],
        [y_upper, x_lower],
    ]


def _bbx_to_napari(y_lower, x_lower, y_upper, x_upper):
    return {"data": [_bbx_to_polygon(y_lower, x_lower, y_upper, x_upper)]}


def _arrange_on_grid(
    imgs: "list[nz.NgffImage]",
) -> tuple["nz.NgffImage", dict[str, Any]]:
    img = imgs[0]
    shape = img.data.shape
    yx_shape = shape[-2:]

    for img in imgs:
        if img.data.shape != shape:
            raise ValueError("Inconsistent image shapes.")

    n_positions = len(imgs)
    grid_size = math.ceil(math.sqrt(n_positions))
    grid_indices = _compute_grid_indices(n_positions, grid_size)
    grid_extent = (grid_indices.max() + 1).rows()[0]

    grid_indices_px = _scale_by_shape(grid_indices, yx_shape)

    yx_canvas_shape = tuple([s * e for s, e in zip(yx_shape, grid_extent)])
    canvas_shape = shape[:-2] + yx_canvas_shape

    canvas_data = da.zeros(canvas_shape, dtype=imgs[0].data.dtype)

    names = []
    bbxs_data = []
    for img, (y, x) in zip(imgs, grid_indices_px.rows()):
        canvas_data[:, :, y : y + yx_shape[0], x : x + yx_shape[1]] = img.data
        names.append(img.name)
        bbxs_data.append(
            to_bbx_polygon(
                img, translate_y=y * img.scale["y"], translate_x=x * img.scale["x"]
            )
        )

    text = {
        "string": "{name}",
        "anchor": "center",
        "translation": [1000, 0, 0],
        "size": 10,
        "color": "white",
    }

    return (
        nz.to_ngff_image(
            canvas_data, dims=img.dims, scale=img.scale, translation=img.translation
        ),
        {
            "data": bbxs_data,
            "features": {"name": names},
            "text": text,
            "face_color": "transparent",
            "edge_color": "white",
            "edge_width": 15,
        },
    )


def _compute_grid_indices(n, grid_size):
    rows = []
    for i in range(n):
        ix = i % grid_size
        iy = i // grid_size
        rows.append([iy, ix])
    return pl.DataFrame(rows, schema=("iy", "ix"), orient="row")


def _scale_by_shape(df: pl.DataFrame, shape: tuple[int, int]):
    return df.select((pl.col("iy") * shape[0]), (pl.col("ix") * shape[1]))
