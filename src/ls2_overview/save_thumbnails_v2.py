import rich_click as click

PHYSICAL_DIMS = ("t", "z", "y", "x")
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
@click.option(
    "-fps",
    "--frames_per_second",
    type=int,
    default=20,
    help="FPS of the resulting movie.",
    show_default=True,
)
@click.option(
    "-o",
    "--out_file",
    type=str,
    default="_thumbnails/thumbnails_v2.mov",
    help="Relative path of the output movie.",
    show_default=True,
)
@click.option(
    "--frames_last_timepoint",
    type=int,
    default=5,
    help="Show the last timepoint for additional frames.",
    show_default=True,
)
def main(
    path: str,
    channel_ids: tuple[int],
    frames_per_second: int,
    out_file: str,
    frames_last_timepoint: int,
):
    """Export a movie of the thumbnails grid for quick inspection."""
    from pathlib import Path

    import napari
    from napari_animation import Animation

    from ls2_overview.utils import to_napari, _arrange_on_grid, load_thumbnail

    def _set_timepoint(viewer, current_timepoint):
        variable_timepoint = list(viewer.dims.current_step)
        variable_timepoint[0] = current_timepoint
        viewer.dims.current_step = variable_timepoint

    def _save_animation(viewer, out_name="demo.mov", fps=5, n_frames_last_timepoint=5):
        n_timepoints = viewer.layers[0].data.shape[0]

        viewer.reset_view()

        animation = Animation(viewer)

        for i in range(n_timepoints):
            _set_timepoint(viewer, i)
            animation.capture_keyframe(steps=1)
        animation.capture_keyframe(steps=n_frames_last_timepoint)
        animation.animate(out_name, canvas_only=True, quality=6, fps=fps)

    experiment_path = Path(path)
    thumbnails_path = experiment_path / "_thumbnails"

    fns_view1 = sorted(list(thumbnails_path.rglob("*View1*")))
    fns_view2 = sorted(list(thumbnails_path.rglob("*View2*")))

    assert len(fns_view1) == len(fns_view2), "Inconsistent number of images."

    imgs_view1 = [load_thumbnail(fn) for fn in fns_view1]
    imgs_view2 = [load_thumbnail(fn) for fn in fns_view2]

    canvas_view1, bbxs = _arrange_on_grid(imgs_view1)
    canvas_view2, _ = _arrange_on_grid(imgs_view2)

    viewer = napari.Viewer()

    viewer.add_image(
        **to_napari(
            canvas_view1,
            channel_ids=channel_ids,
            colormap=VIEW_COLORS[0],
            name="view1",
            blending="additive",
            contrast_limits=CONTRAST_LIMITS,
        )
    )
    viewer.add_image(
        **to_napari(
            canvas_view2,
            channel_ids=channel_ids,
            colormap=VIEW_COLORS[1],
            name="view2",
            blending="additive",
            contrast_limits=CONTRAST_LIMITS,
        )
    )
    viewer.add_shapes(**bbxs)
    viewer.reset_view()

    _save_animation(
        viewer,
        out_name=experiment_path / out_file,
        fps=frames_per_second,
        n_frames_last_timepoint=frames_last_timepoint,
    )
