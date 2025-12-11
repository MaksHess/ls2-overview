# LS2-overview

Small utility to visualize images at the microscope to make descisions about further processing early on.

## Installation

Optional but recommended: create a new conda environment.

```{bash}
conda create --name ls2-overview python=3.12 -y
conda activate ls2-overview
```

Pip install the repo directly from github

```{bash}
pip install git+...
```

## Run the scripts

Available scripts are listed in the `pyproject.toml` under `[project.scripts]`. Usage is explained in the help text accessed, for example, via:

```{bash}
delete_empty_tifs --help
```