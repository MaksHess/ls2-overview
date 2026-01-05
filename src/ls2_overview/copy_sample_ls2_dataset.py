import re
import shutil
from pathlib import Path

from tqdm import tqdm

TIMEPOINTS = range(108, 116)
POSITIONS = ["w1-poisition3_1um-150pls", "w3-poisition1_1um-150pls"]
SOURCE_DIR = Path(
    r"N:\liberali\rawlanding\znascakova\20251209_105714_W1--singlecell--W2-W6--Day3--H2B-mg"
)
TARGET_DIR = Path(r"N:\liberali\rawlanding\hessmax\20251212_1200_SampleExperiment")
RESET_TIME_INDEX = True
T_PATTERN = re.compile(r"-T(\d{4})")


def main():
    global_settings_dir = SOURCE_DIR / "Settings"

    ome_tiff_fns = []
    ome_tiff_companions = []

    for pos in POSITIONS:
        for t in TIMEPOINTS:
            ome_tiff_fns.extend(SOURCE_DIR.glob(f"{pos}*/*-T{t:04d}.tif"))
        ome_tiff_companions.extend(SOURCE_DIR.glob(f"{pos}*/*.ome"))

    shutil.copytree(
        global_settings_dir,
        TARGET_DIR / global_settings_dir.relative_to(SOURCE_DIR),
        dirs_exist_ok=True,
    )
    for fn in ome_tiff_companions:
        target_path = TARGET_DIR / fn.relative_to(SOURCE_DIR)
        target_path.parent.mkdir(exist_ok=True)
        shutil.copy2(fn, target_path)

    for fn in tqdm(ome_tiff_fns):
        target_path = TARGET_DIR / fn.relative_to(SOURCE_DIR)
        target_path.parent.mkdir(exist_ok=True)
        if RESET_TIME_INDEX:
            t_index = int(T_PATTERN.search(fn.name).groups()[0])
            new_t_index = t_index - min(TIMEPOINTS) + 1
            new_name = re.sub(T_PATTERN, f"-T{new_t_index:04d}", target_path.name)
            target_path = target_path.with_name(new_name)
        shutil.copy2(fn, target_path)


if __name__ == "__main__":
    main()
