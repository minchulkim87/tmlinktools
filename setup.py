from pathlib import Path
from setuptools import setup
from setuptools import find_packages

version = '0.0.1'
description = 'Python tools to help build the MarkSTAT - a global trade mark dataset.'

source_root = Path('.')

# Read the requirements
with (source_root / 'requirements.txt').open(encoding='utf-8') as f:
    requirements = f.readlines()

setup(
    name = 'markstattools',
    version = version,
    description = description,
    author = 'Min Chul Kim',
    author_email = 'minchulkim87@gmail.com',
    url = 'https://minchulkim87.github.io/markstattools',
    packages = find_packages(),
    install_requires = requirements
)