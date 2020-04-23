# Crawler

![Docker CI](https://github.com/sanger/crawler/workflows/Docker%20CI/badge.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

This micro service saves sample information from external LIMS into a mongodb instance for easy
querying.

## Requirements

* python - install the required version specified in `Pipfile`:

        [requires]
        python_version = "<version>"

* install the required packages using [pipenv](https://github.com/pypa/pipenv):

        brew install pipenv
        pipenv install --dev

Optionally, to test SFTP, [this](https://hub.docker.com/r/atmoz/sftp/) Docker image is helpful.

## Running

Once all the required packages are installed, enter the virtual environment with:

    pipenv shell

The following runtime flags are available:

    $ python runner.py --help

    usage: runner.py [-h] [--sftp] [--scheduled]

    Store external samples in mongo.

    optional arguments:
    -h, --help   show this help message and exit
    --scheduled  start scheduled execution, defaults to running once
    --sftp       use SFTP to download CSV files, defaults to using local files

## Testing

To run the tests, execute:

    python -m pytest -vs

## Mypy

Mypy is used as a type checker, to execute:

    python -m mypy crawler

## Miscellaneous

### Naming conventions

[This](https://stackoverflow.com/a/45335909) post was used for the naming conventions within mongo.
