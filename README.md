# LS2-overview

Small utility to visualize images at an LS2 microscope to make decisions about further processing early on.

## Installation

Optional but recommended: create a new conda environment.

```{bash}
conda create --name ls2-overview python=3.12 -y
```
```{bash}
conda activate ls2-overview
```
Pip install the repo from github, make sure you have [git](https://git-scm.com/) installed.

```{bash}
pip install git+https://github.com/MaksHess/ls2-overview.git
```

## Run the scripts

Available scripts are listed in the `pyproject.toml` under `[project.scripts]`. Usage is explained in the help text accessed, for example, via:

```{bash}
delete_empty_tifs --help
```
