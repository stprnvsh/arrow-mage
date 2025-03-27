#!/usr/bin/env python3

from setuptools import setup, find_packages

setup(
    name="pipelink",
    version="0.1.0",
    description="Cross-language pipeline orchestration with seamless data sharing",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "pipelink": ["dashboard_templates/*.html", "dashboard_static/*"]
    },
    entry_points={
        "console_scripts": [
            "pipelink=pipelink.cli:main",
            "pipelink-dashboard=pipelink.dashboard_cli:main",
        ],
    },
    install_requires=[
        "pyyaml",
        "pandas",
        "duckdb",
        "numpy",
        "networkx",
        "pyarrow",
        "pyarrow.flight",
        "psutil",
        "flask",
        "matplotlib",
    ],
    python_requires='>=3.7',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
) 