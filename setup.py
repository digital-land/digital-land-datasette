from setuptools import setup
import os

VERSION = "0.6.1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="digital-land-datasette",
    description="Read Parquet files in Datasette",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Colin Dellow",
    url="https://github.com/digital-land/digital-land-datasette",
    project_urls={
        "Issues": "https://github.com/digital-land/digital-land-datasette/issues",
        "CI": "https://github.com/digital-land/digital-land-datasette/actions",
        "Changelog": "https://github.com/digital-land/digital-land-datasette/releases",
    },
    license="Apache License, Version 2.0",
    classifiers=[
        "Framework :: Datasette",
        "License :: OSI Approved :: Apache Software License"
    ],
    version=VERSION,
    packages=["digital_land_datasette"],
    entry_points={"datasette": ["parquet = digital_land_datasette"]},
    install_requires=["datasette", "duckdb", "sqlglot >= 21.2", "watchdog", "boto3","requests","httpfs"],
    extras_require={
        'test': [
            'pytest', 
            'pytest-asyncio', 
            'pytest-watch',
            'pytest-mock',
            'moto',
            'pyarrow',
            'pandas',
        ]
    },
    python_requires=">=3.7",
)
