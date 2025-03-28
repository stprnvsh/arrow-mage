#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name="pipeduck",
    version="0.1.0",
    description="Enhanced Cross-Language Pipeline Orchestration with DuckDB",
    author="Arrow Mage",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "pandas",
        "pyarrow",
        "duckdb",
        "pyyaml",
    ],
    entry_points={
        'console_scripts': [
            'pipeduck=pipeduck.cli:main',
        ],
    },
) 