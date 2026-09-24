.PHONY: test typecheck pyrefly ty
test:
	pytest dramatiq_workflow

typecheck:
	mypy dramatiq_workflow
	pyright dramatiq_workflow

pyrefly:
	pyrefly check --python-interpreter-path "$$(which python)" dramatiq_workflow

ty:
	ty check --python "$$(which python)" dramatiq_workflow

release:
	pip install twine setuptools wheel
	python setup.py sdist bdist_wheel
	twine upload dist/*
