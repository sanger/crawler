#!/bin/bash

###
# A little bash script to ease the formatting and linting burden
#
# run by: ./forlint.sh
###
echo "Running 'black .' on all files using the config in pyproject.toml ..."
black .
echo "Black complete."
echo "---------------"

echo "Running 'mypy .' on all the files using the config in setup.cfg ..."
mypy .
echo "mypy complete."
echo "---------------"

echo "Running 'flake8' on all the files using the config in setup.cfg ..."
flake8
echo "flake8 complete."
