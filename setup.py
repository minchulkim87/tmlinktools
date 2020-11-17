from pathlib import Path
from setuptools import setup
from setuptools import find_packages

version = "0.4.0"
description = "Python tools to help build the TM-Link - a global trade mark dataset."

source_root = Path(".")

with (source_root / "README.md").open(encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    install_requires=[
        "bcrypt==3.2.0; python_version >= '3.6'",
        "bokeh==2.2.3",
        "certifi==2020.11.8",
        "cffi==1.14.3",
        "chardet==3.0.4",
        "click==7.1.2; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3, 3.4'",
        "cloudpickle==1.6.0",
        "cryptography==3.2.1; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3, 3.4'",
        "dask[complete]==2.30.0",
        "distributed==2.30.1",
        "fsspec==0.8.4",
        "heapdict==1.0.1",
        "idna==2.10; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "jinja2==2.11.2",
        "locket==0.2.0",
        "lxml==4.6.1",
        "markupsafe==1.1.1; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "msgpack==1.0.0",
        "numpy==1.19.4",
        "packaging==20.4; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "pandas==1.1.4",
        "pandas-read-xml==0.0.8",
        "paramiko==2.7.2",
        "partd==1.1.0",
        "pillow==8.0.1; python_version >= '3.6'",
        "psutil==5.7.3; python_version >= '2.6' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "pyarrow==0.17.1",
        "pycparser==2.20; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "pynacl==1.4.0; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "pyparsing==2.4.7; python_version >= '2.6' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "python-dateutil==2.8.1; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "python-dotenv==0.15.0",
        "pytz==2020.4",
        "pyyaml==5.3.1",
        "requests==2.25.0; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3, 3.4'",
        "six==1.15.0; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3'",
        "sortedcontainers==2.3.0",
        "tblib==1.7.0; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3, 3.4'",
        "toolz==0.11.1",
        "tornado==6.1; python_version < '3.8'",
        "typing-extensions==3.7.4.3",
        "urllib3==1.26.1; python_version >= '2.7' and python_version not in '3.0, 3.1, 3.2, 3.3, 3.4' and python_version < '4'",
        "xmltodict==0.12.0",
        "zict==2.0.0",
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
