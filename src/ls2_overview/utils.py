from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import ngff_zarr as nz

PHYSICAL_DIMS = ("t", "z", "y", "x")
CONTRAST_LIMITS = (100, 2000)


def to_napari(img: "nz.NgffImage", channel_ids: tuple[int] = (), **kwargs):
    if channel_ids == ():
        return {
            "data": img.data.compute(),
            "scale": [v for k, v in img.scale.items() if k in PHYSICAL_DIMS],
            "translate": [v for k, v in img.translation.items() if k in PHYSICAL_DIMS],
            "channel_axis": None if "c" not in img.dims else img.dims.index("c"),
            **kwargs,
        }
    else:
        assert img.dims.index("c") == 1, (
            "Only arrays of shape ('t', 'c', ...) support `channel_ids`"
        )
        return {
            "data": img.data[:, list(channel_ids), ...].compute(),
            "scale": [v for k, v in img.scale.items() if k in PHYSICAL_DIMS],
            "translate": [v for k, v in img.translation.items() if k in PHYSICAL_DIMS],
            "channel_axis": None if "c" not in img.dims else img.dims.index("c"),
            "contrast_limits": [CONTRAST_LIMITS for _ in range(len(channel_ids))],
            **kwargs,
        }
