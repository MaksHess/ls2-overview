import math
from pathlib import Path
from typing import TYPE_CHECKING

from ls2_overview.utils import to_napari
import rich_click as click

if TYPE_CHECKING:
    import ngff_zarr as nz

experiment_path = Path(r"C:\Users\hessmax\Data\synthetic_data\exp")

VIEW_COLORS = ["green", "magenta"]
GRID_MARGIN = 1.1


@click.command()
@click.argument("path", type=str)
@click.option(
    "-c",
    "--channel_ids",
    type=int,
    multiple=True,
    default=(),
    help="Channel indices to visualize. E.g., `-c 0 -c 1`",
)
def main(path: str, channel_ids: tuple[int]):
    """Visualize thumbnails in a napari viewer."""
    import napari
    import ngff_zarr as nz

    experiment_path = Path(path)
    positions = list((experiment_path / "_thumbnails").glob("*"))

    n_positions = len(positions)

    grid_size = math.ceil(math.sqrt(n_positions))

    dx = dy = 700

    imgs = []
    for i, position in enumerate(positions):
        ix = i % grid_size
        iy = i // grid_size
        for j, view in enumerate(position.glob("*")):
            ngff_img = nz.from_ngff_zarr(view).images[-1]
            if i == 0:
                x_index = ngff_img.dims.index("x")
                y_index = ngff_img.dims.index("y")
                dx = ngff_img.scale["x"] * ngff_img.data.shape[x_index] * GRID_MARGIN
                dy = ngff_img.scale["y"] * ngff_img.data.shape[y_index] * GRID_MARGIN
            ngff_img.translation["x"] += ix * dx
            ngff_img.translation["y"] += iy * dy
            img = to_napari(
                ngff_img,
                channel_ids=channel_ids,
                colormap=VIEW_COLORS[j],
                name=f"{position.name}__{view.stem}",
                blending="additive",
            )
            imgs.append(img)

    viewer = napari.Viewer()
    for img in imgs:
        viewer.add_image(**img)
    viewer.reset_view()
    viewer.show(block=True)
