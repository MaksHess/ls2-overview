import re
import sys
from pathlib import Path

import polars as pl

STACK_PATTERN = re.compile(
    r"(?P<channel>\S+)_(?P<view>View(?P<view_id>\d))-T(?P<t_id>\d+)"
)
POSITION_PATTERN = re.compile(
    r"(?P<acquisition>(?P<position>[^_]+)_(?P<stack>[^_]+))(_(?P<projection>[^_]+))?"
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


def parse_ls2_experiment(experiment_root: Path):
    acc = []
    for path in experiment_root.rglob("*.tif"):
        rel_path = path.relative_to(experiment_root)
        components = {
            **POSITION_PATTERN.match(rel_path.parent.name).groupdict(),
            **STACK_PATTERN.match(rel_path.stem).groupdict(),
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
        pl.col("position")
        .str.split("-")
        .list.get(0)
        .str.replace_all(r"\D", "")
        .cast(pl.UInt8)
        .alias("well_id"),
        pl.col("position")
        .str.split("-")
        .list.get(1)
        .str.replace_all(r"\D", "")
        .cast(pl.UInt8)
        .alias("well_position_id"),
    )

    df_vols = df.filter(pl.col("projection").is_null())
    df_projs = df.filter(pl.col("projection").is_not_null())
    return df_vols, df_projs


if __name__ == "__main__":
    main()
