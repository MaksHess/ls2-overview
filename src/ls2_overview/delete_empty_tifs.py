from pathlib import Path
import click

EMPTY_TIF_SIZE = 100  # n bytes below which a file is empty (header takes some space).
N_PRINT_LINES = 10

@click.command()
@click.argument("root_path", type=str)
@click.option("--no_dry_run", is_flag=True, help="Acually delete files.")
def cli(root_path: str, no_dry_run: bool):
    """ROOT_PATH: Path to LS2 experiment root directory."""
    empty_tifs = detect_empty_tifs(Path(root_path))
    n_empty = len(empty_tifs)
    if n_empty == 0:
        click.echo("No empty .tif files found.")
        return
    else:
        click.echo(f"Found {n_empty} empty .tif files.")
        if no_dry_run:
            click.echo("Deleting empty .tif files.")
            for path in empty_tifs:
                path.unlink()
        else:
            click.echo("Dry run: not deleting files. Use --no_dry_run to delete.")
            print_files(empty_tifs)


def detect_empty_tifs(root_path: Path) -> list[Path]:
    empty_tifs = []
    for path in root_path.rglob("*.tif"):
        if path.stat().st_size < EMPTY_TIF_SIZE:
            empty_tifs.append(path)
    return empty_tifs


def print_files(file_paths: list[Path], n_lines: int = N_PRINT_LINES):
    if len(file_paths) > n_lines:
        for path in file_paths[:n_lines-2]:
            click.echo(f" - {path}")
        click.echo(f" ... and {len(file_paths) - (n_lines-3)} more files.")
        click.echo(f" - {file_paths[-1]}")
    else:
        for path in file_paths:
            click.echo(f" - {path}")