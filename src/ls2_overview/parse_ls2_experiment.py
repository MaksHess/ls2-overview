# %%
from pathlib import Path
import re
import json
import polars as pl

experiment_root = Path(
    r"N:\liberali\rawlanding\znascakova\20251209_105714_W1--singlecell--W2-W6--Day3--H2B-mg"
)
stack_settings_file = "Settings/STAGE_ZStackSettings.json"
stack_pattern = re.compile(r"(?P<channel>\S+)_View(?P<view>\d)-T(?P<timepoint>\d+)")
position_pattern = re.compile(r"(?P<position>\S+)_(?P<acquisition>\S+)")


# %%
path = Path(
    "N:/liberali/rawlanding/znascakova/20251209_105714_W1--singlecell--W2-W6--Day3--H2B-mg/w1-poisition12_1um-150pls/mG_View1-T0072.tif"
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
        }
        acc.append(components)

    return pl.DataFrame(
        acc,
        schema={
            "position": pl.Utf8,
            "acquisition": pl.Utf8,
            "channel": pl.Utf8,
            "view": pl.UInt8,
            "timepoint": pl.UInt16,
            "path": pl.Utf8,
        },
        strict=False,
    ).with_columns(
        pl.col("position")
        .str.split("-")
        .list.get(0)
        .str.replace_all("\D", "")
        .cast(pl.UInt8)
        .alias("well_id"),
        pl.col("position")
        .str.split("-")
        .list.get(1)
        .str.replace_all("\D", "")
        .cast(pl.UInt8)
        .alias("well_position_id"),
    )


# %%
acc = []
for _ in range(10):
    rel_path = path.relative_to(experiment_root)
    components = {
        **position_pattern.match(rel_path.parent.name).groupdict(),
        **stack_pattern.match(rel_path.stem).groupdict(),
        "path": path.as_posix(),
    }
    acc.append(components)

# %%
pl.DataFrame(
        acc,
        schema={
            "position": pl.Utf8,
            "acquisition": pl.Utf8,
            "channel": pl.Utf8,
            "view": pl.UInt8,
            "timepoint": pl.UInt16,
            "path": pl.Utf8,
        },
        strict=False,
    ).with_columns(
        pl.col("position")
        .str.split("-")
        .list.get(0)
        .str.replace_all("\D", "")
        .cast(pl.UInt8)
        .alias("well_id"),
        pl.col("position")
        .str.split("-")
        .list.get(1)
        .str.replace_all("\D", "")
        .cast(pl.UInt8)
        .alias("well_position_id"),
    ).rename({"timepoint": "t_id", "view": "view_id"})

# %%
with open(experiment_root / stack_settings_file) as f:
    stack_settings = json.load(f)
