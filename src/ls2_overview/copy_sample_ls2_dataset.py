import re
import shutil
import tomllib
from argparse import ArgumentParser
from pathlib import Path

from tqdm import tqdm

def main():
    parser = ArgumentParser(description="Copy a sample subset of an LS2 dataset.")
    parser.add_argument("params", type=Path, help="Path to TOML parameter file.")
    args = parser.parse_args()

    with open(args.params, "rb") as f:
        params = tomllib.load(f)

    timepoints = params["timepoints"]
    positions = params["positions"]
    stack_settings = params["stack_settings"]
    source_dir = Path(params["source_dir"])
    target_dir = Path(params["target_dir"])
    reset_time_index = params["reset_time_index"]
    t_pattern = re.compile(params["t_pattern"])
    t_fstring = params["t_fstring"]

    global_settings_dir = source_dir / "Settings"

    ome_tiff_fns = []
    ome_tiff_companions = []

    for pos in positions:
        for stack_setting in stack_settings:
            for t in timepoints:
                ome_tiff_fns.extend(
                    source_dir.glob(f"{pos}_{stack_setting}*/t{t:04d}*.tif")
                )
            ome_tiff_companions.extend(source_dir.glob(f"{pos}_{stack_setting}*/*.ome"))

    shutil.copytree(
        global_settings_dir,
        target_dir / global_settings_dir.relative_to(source_dir),
        dirs_exist_ok=True,
    )
    for fn in ome_tiff_companions:
        target_path = target_dir / fn.relative_to(source_dir)
        target_path.parent.mkdir(exist_ok=True)
        shutil.copy2(fn, target_path)

    for fn in tqdm(ome_tiff_fns):
        target_path = target_dir / fn.relative_to(source_dir)
        target_path.parent.mkdir(exist_ok=True)
        if reset_time_index:
            mo = t_pattern.search(fn.name)
            if mo is None:
                raise ValueError("t_pattern not found in filename.")
            t_index = int(mo.groups()[0])
            new_t_index = timepoints.index(t_index) + 1
            new_name = re.sub(
                t_pattern, t_fstring.format(new_t_index), target_path.name
            )
            target_path = target_path.with_name(new_name)
        shutil.copy2(fn, target_path)


if __name__ == "__main__":
    main()
