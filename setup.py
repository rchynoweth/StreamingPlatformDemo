import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
    
install_reqs = []

setuptools.setup(
    name="dlt-platform",
    version="0.0.1",
    install_requires=install_reqs,
    author="Ryan Chynoweth",
    author_email="ryan.chynoweth@databricks.com",
    description="A package to support streaming workloads on Databricks.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/rchynoweth/StreamingTemplates",
    packages=setuptools.find_packages(),
    classifiers=[],
)