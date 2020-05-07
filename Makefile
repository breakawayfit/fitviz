.PHONY: dev pyspark pyspark-nb notebook lab streamlit env

DEV_ENV_FILE=dev-env.yaml

CONDA_ACTIVATE=source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate
PYTHON_VERSION=$$(sed -nE 's/[[:space:]]*- python=([[:digit:]]\.[[:digit:]])(\.[[:digit:]])*/\1/p' $(DEV_ENV_FILE))

DEV_NAME:=$$(sed -nE 's/[[:space:]]*name:[[:space:]]*([_[:alnum:]]+)/\1/p' $(DEV_ENV_FILE))
DEV=$(CONDA_ACTIVATE) $(DEV_NAME) ;

SPARK_ENV=\
export SPARK_HOME=$$(conda info --base)/envs/$(DEV_NAME)/lib/python$(PYTHON_VERSION)/site-packages/pyspark ; \
export SPARK_LOCAL_IP=127.0.0.1 ; \
export PYSPARK_DRIVER_PYTHON=jupyter ; \
export PYSPARK_DRIVER_PYTHON_OPTS='lab'

dev:
	@conda env remove --name $(DEV_NAME)
	@conda env create -f $(DEV_ENV_FILE)

pyspark:
	@$(DEV) $(SPARK_ENV) ; pyspark

pyspark-nb:
	@$(DEV) $(SPARK_ENV) ; export PYSPARK_DRIVER_PYTHON_OPTS='notebook' ; pyspark

notebook:
	@$(DEV) jupyter notebook

lab:
	@$(DEV) jupyter lab

streamlit:
	@$(DEV) streamlit run streamlit/app.py

env:
	@$(DEV) conda list
