from setuptools import setup, find_packages
import os

setup(
    name='crosslink',
    version='0.1.0',
    author='Your Name', # Consider changing this
    author_email='your.email@example.com', # Consider changing this
    description='CrossLink library for zero-copy data sharing (Python bindings)',
    long_description=open('README.md').read() if os.path.exists('README.md') else '',
    long_description_content_type='text/markdown',
    url='<Your Project URL>', # Optional: Add your project URL
    packages=find_packages(exclude=['examples', 'tests']), # Finds all packages automatically
    install_requires=[
        'pyarrow>=10.0.0', # Check Arrow version compatibility
        'pandas>=1.0.0',
        'duckdb>=0.8.0'   # Check DuckDB version compatibility
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License', # Adjust if needed
        'Operating System :: OS Independent',
        'Development Status :: 3 - Alpha', # Adjust as appropriate
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    python_requires='>=3.8',
) 