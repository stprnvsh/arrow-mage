from setuptools import setup, find_packages

setup(
    name="arrow_cache",
    version="0.1.0",
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
    description="High-performance caching system using Apache Arrow and DuckDB",
    author="Pranav Sateesh",
    python_requires=">=3.8",
)
