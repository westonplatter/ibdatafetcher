CONDA_ENV ?= ibdatafetcher

test:
	@pytest -s .

# release:
# 	@python setup.py sdist
# 	@twine upload dist/*

# example:
# 	@python examples/fetch_.py

env.create:
	@conda create -y -n ${CONDA_ENV} python=3.7

env.update:
	@conda env update -n ${CONDA_ENV} -f environment.yml


##### downloads

download_futures_equities:
	@python download_futures_equities.py

