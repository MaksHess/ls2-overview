from pathlib import Path

import click
import dask.array as da
import ngff_zarr as nz
from dask.array.image import imread
from zarr.codecs import BloscCodec

from ls2_overview.parse_ls2_experiment import parse_ls2_experiment

COMPRESSION_CODEC = BloscCodec()  # Default: 'zstd', clevel=5


@click.command()
@click.argument("path", type=str)
@click.option(
    "-ds", "--down-sample-factor", default=8, help="Down sample factor for thumbnails."
)
@click.option("-xy", "--scale-xy", default=1.0, help="Pixel scale x & y.")
def main(path: str, down_sample_factor: int, scale_xy: float):
    experiment_path = Path(path)
    output_path = experiment_path / "_thumbnails"
    _, df_projections = parse_ls2_experiment(experiment_path)
    for (acquisition, view), df in df_projections.group_by(
        "acquisition", "view", maintain_order=True
    ):
        out_name = output_path / acquisition / f"{view}.ome.zarr"
        channel_arrs = []
        for (channel,), df_ch in df.group_by("channel", maintain_order=True):
            channel_arrs.append(da.stack([imread(e) for e in df_ch["path"]]))

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
            store=out_name,
            multiscales=nz.to_multiscales(ngff_thumbnail, scale_factors=1024),
            version="0.5",
            compressors=[COMPRESSION_CODEC],
        )
