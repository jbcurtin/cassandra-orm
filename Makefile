CIP=$(shell docker inspect cassandra-datastore|jq -r '.[0].NetworkSettings.Networks.bridge.IPAddress')
CIP=127.0.0.1

release:
	pip install -U twine
	pip install -U setuptools
	pip install -U pip
	make clean
	python setup.py sdist
	python -m twine upload --verbose dist/*

clean :
	rm -rf dist
	rm -rf build
	rm -rf corm.egg-info
	rm -rf .tox
    
install: clean
	pip uninstall corm
	python setup.py build
	python setup.py install

build-docs:
	pip install sphinx sphinx_rtd_theme pip setuptools -U
	mkdir -p /tmp/docs
	rm -rf /tmp/docs/*
	sphinx-build -b html docs/ /tmp/docs

tests:
	PYTHONPATH='.' CLUSTER_IPS="$(CIP)" pytest corm-tests/test_corm_api.py -x
	PYTHONPATH='.' CLUSTER_IPS="$(CIP)" pytest corm-tests/test_etl.py -x
