.PHONY: test typecheck
test:
	pytest dramatiq_workflow

typecheck:
	mypy dramatiq_workflow

release:
	pip install twine setuptools wheel
	python setup.py sdist bdist_wheel
	twine upload dist/*
