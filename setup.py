import setuptools

with open("readme.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dynamodb",
    install_requires=[
        "boto3",
    ],
    version="0.0.1",
    author="Joaquin Grez",
    author_email="joaquingrez@consiliumbots.com",
    description=("Thin wrapper for DynamoDB's API"),
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/ConsiliumBots/cb-wrapper-dynamodb",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3.9",
)
