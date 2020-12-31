import sys
from setuptools import setup
from setuptools.command.test import test as TestCommand

with open("README.md", "r") as f:
    long_description = f.read()

setup(
    name="ibdatafetcher",
    version="0.0.1",
    description="ibdatafetcher",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Weston Platter",
    author_email="westonplatter+github@gmail.com",
    url="https://github.com/westonplatter/ibdatafetcher/",
    license="BSD-3-Clause",
    python_requires=">=3.6",
    packages=["ibdatafetcher"],
    project_urls={
        "Issue Tracker": "https://github.com/westonplatter/ibdatafetcher/issues",
        "Source Code": "https://github.com/westonplatter/ibdatafetcher",
    },
)
