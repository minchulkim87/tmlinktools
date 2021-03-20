from pathlib import Path
from setuptools import setup
from setuptools import find_packages

version = "0.4.2"
description = "Python tools to help build the TM-Link - a global trade mark dataset."

source_root = Path(".")

with (source_root / "README.md").open(encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    install_requires=[
        "pandas",
        "xmltodict",
        "dask[complete]",
        "numpy",
        "toolz",
        "lxml",
        "jinja2",
        "python-dotenv",
        "paramiko",
        "pyarrow",
        "zipfile36",
        "pandas-read-xml",
        "urllib3>=1.26.4",
    ],
    name="tmlinktools",
    version=version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Min Chul Kim",
    author_email="minchulkim87@gmail.com",
    url="https://github.com/minchulkim87/tmlinktools",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
