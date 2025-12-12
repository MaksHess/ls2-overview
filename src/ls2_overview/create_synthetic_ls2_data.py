import itertools
import opensimplex
import numpy as np
import tifffile
from pathlib import Path
import click

@click.command()
@click.argument("output_dir", type=str)
def cli(output_dir: str):
    """OUTPUT_DIR: Path to output directory."""
    experiment_dir = Path(output_dir)

    t, z, y, x = 30, 20, 512, 512  # z only for naming, writing max-projections

    positions = ["w1-p3", "w2-p1", "w2-p2", "w3-p1", "w3-p2"]
    channels = ["mG", "H2B"]
    views = ["View1", "View2"]

    c = len(channels)

    acquisitions = [f"1um-{z}pls"]

    full_positions = [
        f"{p}_{acq}_maxZ" for p, acq in itertools.product(positions, acquisitions)
    ]
    rng = np.random.default_rng(33)

    for i, position in enumerate(full_positions):
        print(f"Position: {position}")
        position_dir = Path(experiment_dir) / position
        position_dir.mkdir(parents=True, exist_ok=True)

        opensimplex.seed(i)
        extent_multiplier = rng.random() * 0.8 + 0.2  # between 0.2 and 1.0
        xy_extent = 10.0 * extent_multiplier
        t_extent = 1.0
        c_extent = 0.3

        view_shift = xy_extent / 15

        for j, view in enumerate(views):
            print(f"  View: {view}")
            arr = opensimplex.noise4array(
                np.linspace(0 + j*view_shift, xy_extent + j*view_shift, x),
                np.linspace(0, xy_extent, y),
                np.linspace(0, c_extent, c),
                np.linspace(0, t_extent, t),
            )
            for k, channel in enumerate(channels):
                print(f"    Channel: {channel}")
                arr_ch = arr[:, k, :, :]
                for m, arr_ch_t in enumerate(arr_ch):
                    print(f"      Timepoint: {m}")
                    filename = f"{channel}_{view}-T{m:04d}.tif"
                    tifffile.imwrite(position_dir / filename, arr_ch_t)
