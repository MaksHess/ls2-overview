import rich_click as click
from time import time

VIEW_COLORS = ["green", "magenta"]
GRID_MARGIN = 1.1
CONTRAST_LIMITS = (100, 2000)


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

    start = time()
    print("importing...", end="\t", flush=True)
    from pathlib import Path
    import napari
    from ls2_overview.utils import to_napari, _arrange_on_grid, load_thumbnail

    t1 = time()
    print(f"{round(t1 - start):d}s")

    print("loading metadata...", end="\t", flush=True)
    experiment_path = Path(path)
    thumbnails_path = experiment_path / "_thumbnails"

    fns_view1 = sorted(list(thumbnails_path.rglob("*View1*")))
    fns_view2 = sorted(list(thumbnails_path.rglob("*View2*")))

    assert len(fns_view1) == len(fns_view2), "Inconsistent number of images."

    imgs_view1 = [load_thumbnail(fn) for fn in fns_view1]
    imgs_view2 = [load_thumbnail(fn) for fn in fns_view2]

    t2 = time()
    print(f"{round(t2 - t1)}s")

    print("arranging canvases...", end="\t", flush=True)
    canvas_view1, bbxs = _arrange_on_grid(imgs_view1)
    canvas_view2, _ = _arrange_on_grid(imgs_view2)

    t3 = time()
    print(f"{round(t3 - t2)}s")

    print("loading arrays...", end="\t", flush=True)

    napari_view1 = to_napari(
        canvas_view1,
        channel_ids=channel_ids,
        colormap=VIEW_COLORS[0],
        name="view1",
        blending="additive",
        contrast_limits=CONTRAST_LIMITS,
    )

    napari_view2 = to_napari(
        canvas_view2,
        channel_ids=channel_ids,
        colormap=VIEW_COLORS[1],
        name="view2",
        blending="additive",
        contrast_limits=CONTRAST_LIMITS,
    )

    t4 = time()
    print(f"{round(t4 - t3)}s")

    print("starting viewer...", end="\t", flush=True)
    viewer = napari.Viewer()

    viewer.add_image(**napari_view1)
    viewer.add_image(**napari_view2)
    viewer.add_shapes(**bbxs)
    viewer.reset_view()

    t5 = time()
    print(f"{round(t5 - t4)}s")

    viewer.show(block=True)
