from setuptools import setup, find_packages

# Read the README.md file for the long description
with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="arrow_cache",
    version="0.2.2",
    packages=find_packages(),
    install_requires=[
        "pyarrow>=10.0.0",
        "duckdb>=0.7.0",
        "pandas>=1.0.0",
        "numpy>=1.20.0",
    ],
    extras_require={
        "geo": ["geopandas>=0.9.0", "pyogrio>=0.4.0"],
        "parquet": ["pyarrow-parquet>=10.0.0"],
    },
    entry_points={
        "console_scripts": [
            "arrow-cache-cleanup=arrow_cache.utils:cleanup_command",
        ],
    },
    description="High-performance caching system using Apache Arrow and DuckDB",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Pranav Sateesh",
    author_email="pranav.sateesh@usi.ch",  # Add your email or use a placeholder
    url="https://github.com/stprnvsh/arrow-cache",  # Add your repository URL
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
