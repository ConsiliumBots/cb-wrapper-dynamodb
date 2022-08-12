import setuptools

with open("readme.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dynamodb",
    install_requires=[
        "boto3",
        "logdna",
    ],
    version="0.4.2",
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

# 0.0.1 -> Initial
# 0.0.2 -> Env var as optionals
# 0.0.3 -> Secrets manager
# 0.0.4 -> Fix en level
# 0.0.5 -> Fix
# 0.1.0 -> Major update on functions
# 0.1.1 -> AWS secrets added
# 0.1.1 -> AWS secrets removed
# 0.1.4 -> Method get_column_count
# 0.2.0 -> Dynamo class refactor
# 0.2.1 -> Error fixed
# 0.3.1 -> NotFoundError handled
# 0.4.1 -> Error handler for get_column_count method
# 0.4.2 -> str index added
