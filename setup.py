#!/usr/bin/python3


from setuptools import setup, find_packages


setup(
    name="prcc",
    version="0.1",
    packages=find_packages(),
    scripts=["bin/prcc"],
    author="Felipe S. S. Schneider",
    author_email="schneider.felipe.5@gmail.com",
    description=("Personal daily time series storage for stock market and funds"),
    long_description=open("README.rst").read(),
    install_requires=[
        "numpy",
        "pandas",
        "pandas_datareader",
        "pystore",
        "unidecode",
        "requests_cache",
    ],
    license="MIT",
    url="https://github.com/schneiderfelipe/prcc",
    keywords="stock-market time-series",
    classifiers=[
        "Programming Language :: Python",
        "License :: OSI Approved :: MIT License",
        "Development Status :: 3 - Alpha",
    ],
)
