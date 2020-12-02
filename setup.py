import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

PACKAGE_DEPENDENCIES = ["altair", "pandas"]

setuptools.setup(
    name="fitviz",
    version="1.0.0",
    author="John M Donich",
    author_email="code@johnmdonich.com",
    description="Data visualizations for cycling data",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/johnmdonich/fitviz.git",
    packages=setuptools.find_packages(),
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
    install_requires=PACKAGE_DEPENDENCIES,
)
