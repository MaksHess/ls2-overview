import sys
from pathlib import Path

import rich_click as click
import dask.array as da
import ngff_zarr as nz
import polars as pl
from dask.array.image import imread
from zarr.codecs import BloscCodec

# from ls2_overview.parse_ls2_experiment import parse_ls2_experiment
from ls2_overview.parse_ls2_experiment_lightsheet_fusion import parse_ls2_experiment


if sys.platform.startswith("win"):
    import zarr

    zarr.config.set({"threading": {"max_workers": 1}})
    zarr.config.set({"async": {"concurrency": 1, "timeout": None}})

ZARR_V05 = {
    "compressors": [BloscCodec()],  # Default: 'zstd', clevel=5
    "version": "0.5",
}

ZARR_V04 = {
    # "compression": "zstd",
    "version": "0.4"
}


@click.command()
@click.argument("path", type=str)
@click.option(
    "-ds",
    "--down-sample-factor",
    default=8,
    help="Down sample factor.",
    show_default=True,
)
@click.option(
    "-c",
    "--channels",
    default=("*",),
    multiple=True,
    help="Channels to use in thumbnails. Select multiple using `-c H2B -c mG`.",
    show_default=True,
)
@click.option("-xy", "--scale-xy", default=1.0, help="Pixel scale.", show_default=True)
@click.option(
    "--ozx",
    is_flag=True,
    help="Wheter to write thumbnails as zipped zarr file (.ozx).",
    show_default=True,
)
def main(path: str, down_sample_factor: int, scale_xy: float, channels: str, ozx: bool):
    "Compute thumbnails & store them to a directory `_thumbnails`."
    experiment_path = Path(path)
    thumbnails_path = experiment_path / "_thumbnails"
    thumbnails_path.mkdir(exist_ok=True)

    _, df_projections = parse_ls2_experiment(experiment_path)
    if channels == ("*",):
        channels = tuple(df_projections["channel"].unique().sort())
    else:
        df_projections = df_projections.filter(pl.col("channel").is_in(channels))
    df_projections = df_projections.with_columns(
        pl.col("channel")
        .replace(dict(e[::-1] for e in enumerate(channels)), return_dtype=pl.UInt8)
        .alias("channel_id")
    )
    for (acquisition, view), df in df_projections.group_by(
        "acquisition", "view", maintain_order=True
    ):
        if ozx:
            out_name = f"{view}.ozx"
        else:
            out_name = f"{view}.ome.zarr"
        out_path = thumbnails_path / acquisition / out_name
        out_path.parent.mkdir(exist_ok=True)
        channel_arrs = []
        for (channel,), df_ch in df.sort(by=["channel_id", "t_id"]).group_by(
            "channel", maintain_order=True
        ):
            timepoints = []
            for i, path in enumerate(df_ch["path"]):
                t_arr = imread(path)
                if i > 0:
                    if t_arr.shape != timepoints[0].shape:
                        t_arr = da.zeros_like(timepoints[0])
                timepoints.append(t_arr)
            channel_arrs.append(da.stack(timepoints))
        arr = da.concatenate(channel_arrs, axis=1)

        ngff_image = nz.to_ngff_image(
            arr,
            dims=["t", "c", "y", "x"],
            scale={"y": scale_xy, "x": scale_xy},
        )

        ngff_multiscales = nz.to_multiscales(
            ngff_image,
            scale_factors=[
                {"x": down_sample_factor, "y": down_sample_factor},
            ],
            chunks=(1, 1, None, None),
            # zarr_store_kwargs={"chunks": (1, 1, None, None)},
        )
        ngff_thumbnail = ngff_multiscales.images[-1]
        nz.to_ngff_zarr(
            store=out_path,
            multiscales=nz.to_multiscales(ngff_thumbnail, scale_factors=1024),
            **ZARR_V05,
        )
