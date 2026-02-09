import json
import re
import sys
import warnings
from pathlib import Path

import polars as pl
from packaging.version import Version, parse

STACK_PATTERN_V2 = re.compile(
    r"(?P<channel>\S+)_(?P<view>View(?P<view_id>\d))-[Tt](?P<t_id>\d+)"
)
STACK_PATTERN_V2B = re.compile(
    r"[Tt](?P<t_id>\d+)_(?P<channel>\S+)_(?P<view>View(?P<view_id>\d))"
)
POSITION_PATTERN_V2 = re.compile(
    r"(?P<acquisition>(?P<position>[^_]+)_(?P<stack>[^_]+))(_(?P<projection>[^_]+))?"
)

STACK_AND_POSITION_PATTERN_V3 = re.compile(
    r"^(?P<acquisition>(?P<position>[^_]+)_(?P<stack>[^_]+))(_Optional\/(?P<projection>[^_]+))?\/[Tt](?P<t_id>\d+)_(?P<channel>[^_]+)_(?P<view>View(?P<view_id>\d)).tif$"
)


def main():
    if len(sys.argv) != 2:
        print("Usage: python parse_ls2_experiment.py <experiment_root>")
        sys.exit(1)
    experiment_root = Path(sys.argv[1])
    df_vols, df_projs = parse_ls2_experiment(experiment_root)
    print(df_vols)
    print(df_projs)
    sys.exit(0)


def parse_ls2_experiment(experiment_root: Path | str):
    experiment_root = Path(experiment_root)
    version = parse_ls2_version(experiment_root)
    if version.major == 2:
        return parse_ls2_experiment_v2(experiment_root)
    elif version.major == 3:
        return parse_ls2_experiment_v3(experiment_root)
    else:
        raise NotImplementedError("Unsupported version {version}.")


def parse_ls2_experiment_v2(experiment_root: Path):
    acc = []
    for path in experiment_root.rglob("*.tif"):
        rel_path = path.relative_to(experiment_root)
        position_mo = POSITION_PATTERN_V2.match(rel_path.parent.name)
        stack_mo = STACK_PATTERN_V2.match(rel_path.stem)
        if stack_mo is None:
            stack_mo = STACK_PATTERN_V2B.match(rel_path.stem)
        if position_mo is None:
            warnings.warn(f"Folder did not match regex: {rel_path.parent}")
        elif stack_mo is None:
            warnings.warn(f"Filename did not match regex: {rel_path.stem}")
        else:
            components = {
                **position_mo.groupdict(),
                **stack_mo.groupdict(),
                "path": path.as_posix(),
                "tif_file_size": path.stat().st_size,
            }
            acc.append(components)

    df = pl.DataFrame(
        acc,
        schema={
            "acquisition": pl.String,
            "position": pl.String,
            "stack": pl.String,
            "projection": pl.String,
            "channel": pl.String,
            "view": pl.String,
            "view_id": pl.UInt8,
            "t_id": pl.UInt16,
            "path": pl.String,
            "tif_file_size": pl.UInt64,
        },
        strict=False,
    ).with_columns(
        pl.col("t_id") - pl.col("t_id").min(),
    )

    df_vols = df.filter(pl.col("projection").is_null())
    df_projs = df.filter(pl.col("projection").is_not_null())
    return df_vols, df_projs


def parse_ls2_experiment_v3(experiment_root: Path):
    acc = []
    for path in experiment_root.rglob("*.tif"):
        rel_path = path.relative_to(experiment_root)
        components = {
            **STACK_AND_POSITION_PATTERN_V3.match(rel_path.as_posix()).groupdict(),
            "path": path.as_posix(),
            "tif_file_size": path.stat().st_size,
        }
        acc.append(components)

    df = pl.DataFrame(
        acc,
        schema={
            "acquisition": pl.String,
            "position": pl.String,
            "stack": pl.String,
            "projection": pl.String,
            "channel": pl.String,
            "view": pl.String,
            "view_id": pl.UInt8,
            "t_id": pl.UInt16,
            "path": pl.String,
            "tif_file_size": pl.UInt64,
        },
        strict=False,
    ).with_columns(
        pl.col("t_id") - pl.col("t_id").min(),
    )

    df_vols = df.filter(pl.col("projection").is_null())
    df_projs = df.filter(pl.col("projection").is_not_null())
    return df_vols, df_projs


def parse_ls2_version(experiment_root: Path) -> Version:
    dir_settings = experiment_root / "Settings"
    settings_file = next(dir_settings.glob("*.json"))
    with open(settings_file) as f:
        settings = json.load(f)
    version = parse(settings["Version"])
    return version


# %%
if __name__ == "__main__":
    main()
