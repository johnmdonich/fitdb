# Conda Environment Starter

A simple starter for a `conda` development environment with a consistent and repeatable api to rebuild, test, and deploy with `make`. It provides a convenient way to have conda environment from within a folder reduce environment switching.

## Usage

### Environment file

A simple conda environment is located at `env-dev.yaml`.
Update the name of the environment to match your project at the top of the file.

```yaml
name: scistack-basic # Replace this name 
dependencies:
    ...
```

## Make Commands

If you add make command adding the `@$(DEV)` before the command statement will activate the environment before executing the the procedure.

| Comand        | Usage                                                       |
| :------------ | :---------------------------------------------------------- |
| `cov`         | Opens the coverage report from pytest-cov in Google Chrome  |
| `docs`        | Runs an mkdocs development server                           |
| `docs-deploy` | Deploys mkdocs to the repo's pages                          |
| `env-dev`     | (Re)builds the conda environment in env-dev.yaml with mamba |
| `env-info`    | List the contents of the conda environment                  |
| `lab`         | Runs a vanilla JupyterLab server (VS Code is close)         |
| `test`        | A place holder for pytest command                           |
| `types`       | A place holder for mypy command                             |

## Prerequisite Install Mambaforge

Mambaforge is a conda distribution of python that pulls package

### Apple Silicon

Download Mambaforge Installer

```zsh
curl -L "https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-MacOSX-arm64.sh" -o $HOME/Mambaforge-MacOSX-arm64.sh
```

Install in `$HOME`

```zsh
bash $HOME/Mambaforge-MacOSX-arm64.sh
```

### Apple Intel

Download Mambaforge Installer

```zsh
curl -L "https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-MacOSX-x86_64.sh" -o $HOME/Mambaforge-MacOSX-arm64.sh
```

Install in `$HOME`

```zsh
bash $HOME/Mambaforge-MacOSX-arm64.sh
```

