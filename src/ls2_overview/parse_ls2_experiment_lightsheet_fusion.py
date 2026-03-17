# TODO: Duplicated from lightsheet-fusion repo. Delete once the two are merged.
import json
import logging
import re
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl
from packaging.version import Version
from packaging.version import parse as parse_version

logger = logging.getLogger(__name__)

SETTINGS_REL_PATH = "Settings"
STORAGE_SETTINGS_FILE = "CAM_StorageSettings.json"

DEFAULT_SPATIAL_UNIT = "micrometer"
DEFAULT_TIME_UNIT = "second"

UNIT_MAPPING = {
    "nm": "nanometer",
    "µm": "micrometer",
    "um": "micrometer",
    "s": "second",
    "ms": "millisecond",
    "min": "minute",
    "hr": "hour",
    "h": "hour",
}

# V2 has inconsistent file naming that can be configured and are
# stored in Settings/CAM_StorageSettings.json
DIRECTORY_REPLACEMENT_PATTERNS: dict[str, str] = {
    "[P]": r"(?P<position>[^_]+)",
    "[S]": r"(?P<stack>[^_]+)",
}

FILE_REPLACEMENT_PATTERNS: dict[str, str] = {
    "[CH]": r"(?P<channel>[^_]+)(_(?P<view>View(?P<view_id>\d)))?",
    "[T]": r"(?P<t_id>\d+)",
}

# Example pattern for V2 data encountered in the wild
EXAMPLE_PATTERN_V2 = r"^(?P<acquisition>(?P<position>[^_]+)_(?P<stack>[^_]+))(_(?P<projection>[^_]+))?\/[Tt](?P<t_id>\d+)_(?P<channel>[^_]+)_(?P<view>View(?P<view_id>\d)).tif$"

# V3 pattern, should be stable
PATTERN_V3 = r"^(?P<acquisition>(?P<position>[^_]+)_(?P<stack>[^_]+))(_Optional\/(?P<projection>[^_]+))?\/[Tt](?P<t_id>\d+)_(?P<channel>[^_]+)_(?P<view>View(?P<view_id>\d)).tif$"


@dataclass(frozen=True)
class Config:
    """Application configuration. Usually not touched by the user."""

    cache_dir: str = ".lightsheet-fusion"
    """Directory to write cache to."""

    empty_tif_file_size: int = 32
    """File size of an empty tif."""

    crop_pixels_multiple_of: int = 32
    """Cropped regions must be a multiple of this."""

    positions_cache_file: str = "positions.parquet"
    """Cache file for parsed positions. One file per experiment."""


config = Config()


def _replace_all(text, dic):
    for i, j in dic.items():
        text = text.replace(i, j)
    return text


def parse_storage_settings(
    experiment_root: Path | str,
    version: Version,
) -> re.Pattern:
    fn = Path(experiment_root) / SETTINGS_REL_PATH / STORAGE_SETTINGS_FILE
    logger.debug(f"Loading storage settings from: {fn}")
    with open(fn) as f:
        data = json.load(f)

    directory_setting = str(data["Items"][0]["Directory"])
    file_setting = str(data["Items"][0]["File"])

    directory_pattern = _replace_all(directory_setting, DIRECTORY_REPLACEMENT_PATTERNS)
    file_pattern = _replace_all(file_setting, FILE_REPLACEMENT_PATTERNS)

    if version.major == 2:
        pattern = rf"(?P<acquisition>{directory_pattern})(_(?P<projection>[^_]+))?\/{file_pattern}.tif"
    elif version.major == 3:
        # pattern = rf"(?P<acquisition>{directory_pattern})(_Optional\/(?P<projection>[^_]+))?\/{file_pattern}.tif"
        pattern = PATTERN_V3
    else:
        raise ValueError(f"Unsupported version: {version}")
    return re.compile(pattern)


def parse_ls2_experiment(
    experiment_root: Path | str,
    position: str | None = None,
    ignore_cache: bool = False,
    pattern: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    logger.debug(f"Parsing experiment: {experiment_root}")

    cache_file = Path(experiment_root) / config.cache_dir / config.positions_cache_file
    cache_file.parent.mkdir(exist_ok=True)

    if ignore_cache or not cache_file.exists():
        if pattern is None:
            df_vols, df_projs = _parse_ls2_experiment(
                experiment_root=experiment_root,
                include_file_size=True,  # keep caching simple by always including file size
                position=None,  # ... and by only allowing parsing all positions
            )
        else:
            df_vols, df_projs = _parse_ls2_experiment_with_regex(
                experiment_root=Path(experiment_root),
                regex=re.compile(pattern),
                include_file_size=True,  # keep caching simple by always including file size
                position=None,  # ... and by only allowing parsing all positions
            )
        logger.debug(f"Writing parsing results to: {cache_file}")
        pl.concat([df_vols, df_projs]).write_parquet(cache_file)
    else:
        logger.debug(f"Loading parsing results from: {cache_file}")
        df_all = pl.read_parquet(cache_file)
        df_vols = df_all.filter(pl.col("projection").is_null())
        df_projs = df_all.filter(pl.col("projection").is_not_null())

    if position is not None:
        df_vols = df_vols.filter(pl.col("position") == position)
        df_projs = df_projs.filter(pl.col("position") == position)
    logger.debug(
        f"Volume frames: {len(df_vols)} Empty: {len(df_vols.filter(pl.col('tif_file_size') <= config.empty_tif_file_size))}"
    )
    logger.debug(
        f"Max projections: {len(df_projs)} Empty: {len(df_projs.filter(pl.col('tif_file_size') <= config.empty_tif_file_size))}"
    )
    return df_vols, df_projs


def _parse_ls2_experiment(
    experiment_root: Path | str,
    include_file_size: bool = False,
    position: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    experiment_root = Path(experiment_root)

    version = parse_ls2_version(experiment_root)
    regex = parse_storage_settings(
        experiment_root,
        version=version,
    )
    df_volumes, df_projs = _parse_ls2_experiment_with_regex(
        experiment_root,
        regex=regex,
        include_file_size=include_file_size,
        position=position,
    )
    return df_volumes, df_projs


def _parse_ls2_experiment_with_regex(
    experiment_root: Path,
    regex: re.Pattern[str],
    include_file_size: bool = False,
    position: str | None = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if position is None:
        glob_pattern = "**/*.tif"
    else:
        glob_pattern = f"**{position}*/*.tif"
    fns = list(experiment_root.glob(glob_pattern))
    acc = []
    for path in fns:
        rel_path = path.relative_to(experiment_root)
        mo = regex.match(rel_path.as_posix())
        if mo is None:
            warnings.warn(f"File path {rel_path} does not match expected pattern.")
        else:
            components = {
                **mo.groupdict(),
                "path": path.as_posix(),
            }
            if include_file_size:
                components["tif_file_size"] = path.stat().st_size
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


def parse_experiment_ome_metas(experiment_root: Path | str) -> pl.DataFrame:
    ome_metas = []
    for path in Path(experiment_root).rglob("*.companion.ome"):
        ome_metas.extend(parse_ome_metadata(path))
    return pl.DataFrame(ome_metas)


def parse_ls2_version(experiment_root: Path | str) -> Version:
    """Parse LS2 experiment version from Settings JSON file."""
    settings_dir = Path(experiment_root) / SETTINGS_REL_PATH
    if not settings_dir.exists():
        raise FileNotFoundError("`Settings` folder does not exist.")
    settings_file = next(settings_dir.glob("*.json"))  # all settings contain version.
    with open(settings_file) as f:
        settings = json.load(f)
    version = parse_version(settings["Version"])
    logger.debug(f"LS2 Version: {version}")
    return version


def parse_ome_metadata(companion_file: str | Path) -> list[dict[str, Any]]:
    """Parse OME-XML companion file and return list of metadata dictionaries."""
    ome_dict = _parse_ome_metadata(companion_file)
    metadatas = _extract_metadata(ome_dict)
    return metadatas


def _parse_ome_metadata(companion_file: str | Path) -> dict:
    """Parse OME-XML companion file and return metadata as a dictionary."""
    import tifffile

    with open(companion_file, encoding="utf-8-sig") as f:
        data = f.read()
    ome_dict = tifffile.xml2dict(data)
    return ome_dict


def _extract_metadata(ome_dict: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract relevant metadata from OME dictionary."""
    ome = ome_dict.get("OME", {})
    images = ome.get("Image", [])
    if not isinstance(images, list):
        images = [images]

    metadatas = []
    for image in images:
        metadata: dict[str, Any] = {}
        metadata["name"] = image.get("Name")
        pixels = image.get("Pixels", {})
        metadata["shape_x"] = int(pixels.get("SizeX", 1))
        metadata["shape_y"] = int(pixels.get("SizeY", 1))
        metadata["shape_z"] = int(pixels.get("SizeZ", 1))
        metadata["shape_c"] = int(pixels.get("SizeC", 1))
        metadata["shape_t"] = int(pixels.get("SizeT", 1))

        metadata["scale_x"] = float(pixels.get("PhysicalSizeX", 1))
        metadata["scale_y"] = float(pixels.get("PhysicalSizeY", 1))
        metadata["scale_z"] = float(pixels.get("PhysicalSizeZ", 1))
        metadata["scale_t"] = float(pixels.get("TimeIncrement", 1))

        unit_x = pixels.get("PhysicalSizeXUnit", DEFAULT_SPATIAL_UNIT)
        unit_y = pixels.get("PhysicalSizeYUnit", DEFAULT_SPATIAL_UNIT)
        unit_z = pixels.get("PhysicalSizeZUnit", DEFAULT_SPATIAL_UNIT)
        unit_t = pixels.get("TimeIncrementUnit", DEFAULT_TIME_UNIT)

        metadata["unit_x"] = UNIT_MAPPING.get(unit_x, unit_x)
        metadata["unit_y"] = UNIT_MAPPING.get(unit_y, unit_y)
        metadata["unit_z"] = UNIT_MAPPING.get(unit_z, unit_z)
        metadata["unit_t"] = UNIT_MAPPING.get(unit_t, unit_t)

        metadatas.append(metadata)
    return metadatas
