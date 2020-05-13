from pathlib import Path
from setuptools import setup
from setuptools import find_packages

version = "0.1.4"
description = "Python tools to help build the MARKSTAT - a global trade mark dataset."

source_root = Path(".")

with (source_root / "README.md").open(encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    install_requires=[
        "bokeh==2.0.2",
        "certifi==2020.4.5.1",
        "chardet==3.0.4",
        "click==7.1.2",
        "cloudpickle==1.4.1",
        "dask[complete]==2.16.0",
        "distributed==2.16.0",
        "fsspec==0.7.3",
        "heapdict==1.0.1",
        "idna==2.9",
        "jinja2==2.11.2",
        "locket==0.2.0",
        "markupsafe==1.1.1",
        "msgpack==1.0.0",
        "numpy==1.18.4",
        "packaging==20.3",
        "pandas==1.0.3",
        "pandas-read-xml==0.0.5",
        "partd==1.1.0",
        "pillow==7.1.2",
        "psutil==5.7.0",
        "pyarrow==0.17.0",
        "pyparsing==2.4.7",
        "python-dateutil==2.8.1",
        "pytz==2020.1",
        "pyyaml==5.3.1",
        "requests==2.23.0",
        "six==1.14.0",
        "sortedcontainers==2.1.0",
        "tblib==1.6.0",
        "toolz==0.10.0",
        "tornado==6.0.4; python_version < '3.8'",
        "typing-extensions==3.7.4.2",
        "urllib3==1.25.9",
        "xmltodict==0.12.0",
        "zict==2.0.0",
    ],
    name="markstattools",
    version=version,
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Min Chul Kim",
    author_email="minchulkim87@gmail.com",
    url="https://github.com/minchulkim87/markstattools",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
