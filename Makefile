.PHONY: test
test:
	pytest dramatiq_workflow

release:
	pip install twine setuptools wheel
	python setup.py sdist bdist_wheel
	twine upload dist/*
