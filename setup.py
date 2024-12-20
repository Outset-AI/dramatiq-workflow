from setuptools import find_packages, setup

setup(
    name="dramatiq-workflow",
    version="0.2.0",
    description="A library for running workflows (chains and groups of tasks) using the Python background task processing library dramatiq.",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="Outset",
    author_email="engineering@outset.ai",
    url="https://github.com/Outset-AI/dramatiq-workflow",
    packages=find_packages(),
    install_requires=[
        "dramatiq>=1.10.0",
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
)
