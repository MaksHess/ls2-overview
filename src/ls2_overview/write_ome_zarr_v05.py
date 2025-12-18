# %%
from pathlib import Path
import numpy as np
from ngio import create_ome_zarr_from_array
import zarr

# TODO: remove temporary workaround for zarr issues on windows
# see: https://github.com/zarr-developers/zarr-python/issues/3522
zarr.config.set({"threading": {"max_workers": 1}})
zarr.config.set({"async": {"concurrency": 1, "timeout": None}})


def main():
    path = Path(r"C:\Users\hessmax\Data\synthetic_zarr")

    arr = np.random.randint(0, 255, (10, 2, 64, 256, 256))
    arr = (arr * np.linspace(0.3, 1.0, 10).reshape(10, 1, 1, 1, 1)).astype(np.uint8)

    # %% write without sharding
    create_ome_zarr_from_array(
        store=path / "data.ome.zarr",
        array=arr,
        axes_names="tczyx",
        pixelsize=(0.24, 0.24),
        z_spacing=2.0,
        channel_labels=["mG", "H2B"],
        chunks=(1, 1, 64, 256, 256),
        ngff_version="0.5",
        overwrite=True,
    )

    # %% write with sharding
    create_ome_zarr_from_array(
        store=path / "data_sharded.ome.zarr",
        array=arr,
        axes_names="tczyx",
        pixelsize=(0.24, 0.24),
        z_spacing=2.0,
        channel_labels=["mG", "H2B"],
        chunks=(1, 1, 8, 128, 128),
        shards=(1, 1, 64, 256, 256),
        ngff_version="0.5",
        overwrite=True,
    )


if __name__ == "__main__":
    main()
