.PHONY: cov docs docs-deploy env-dev env-info lab

DEV_ENV_FILE=env-dev.yaml

CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate
PYTHON_VERSION=$$(sed -nE 's/[[:space:]]*- python=([[:digit:]]\.[[:digit:]])(\.[[:digit:]])*/\1/p' $(DEV_ENV_FILE))

DEV_NAME:=$$(sed -nE 's/[[:space:]]*name:[[:space:]]*([_[:alnum:]]+)/\1/p' $(DEV_ENV_FILE))
DEV=$(CONDA_ACTIVATE) $(DEV_NAME) ;

cov:
	open -a "Google Chrome" htmlcov/index.html

docs:
	@$(DEV) mkdocs serve

docs-deploy:
	@$(DEV) mkdocs gh-deploy

env-dev:
	@mamba env remove --name $(DEV_NAME)
	@mamba env create -f $(DEV_ENV_FILE)

env-info:
	@$(DEV) conda list

lab:
	@$(DEV) jupyter lab

test:
	@$(DEV) echo "Not Implemented: add pytest command in Makefile"

types:
	@$(DEV) echo "Not Implemented: add mypy command in Makefile"