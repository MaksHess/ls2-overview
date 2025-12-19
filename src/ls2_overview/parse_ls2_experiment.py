# %%
from pathlib import Path
import re
import json
import polars as pl


experiment_root = Path(
    r"Z:\rawlanding\znascakova\20251209_105714_W1--singlecell--W2-W6--Day3--H2B-mg"
)
stack_settings_file = "Settings/STAGE_ZStackSettings.json"
stack_pattern = re.compile(
    r"(?P<channel>\S+)_(?P<view>View(?P<view_id>\d))-T(?P<t_id>\d+)"
)
position_pattern = re.compile(
    r"(?P<position>[^_]+)_(?P<stack>[^_]+)(_(?P<projection>[^_]+))?"
)


# %%
def parse_ls2_experiment(experiment_root: Path):
    acc = []
    for path in experiment_root.rglob("*.tif"):
        rel_path = path.relative_to(experiment_root)
        components = {
            **position_pattern.match(rel_path.parent.name).groupdict(),
            **stack_pattern.match(rel_path.stem).groupdict(),
            "path": path.as_posix(),
            "tif_file_size": path.stat().st_size,
        }
        acc.append(components)

    df = pl.DataFrame(
        acc,
        schema={
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


# %%
df, df_projs = parse_ls2_experiment(experiment_root)

# %%
print(
    df.filter(pl.col("projection").is_null())
    .group_by("position", "stack", "view", "channel", maintain_order=True)
    .agg(pl.len())
)
