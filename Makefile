test:
	python setup.py test

pypi:
	python setup.py sdist bdist_egg upload
