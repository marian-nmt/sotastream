# Docs


## Build Docs
```bash
pip install -U sphinx sphinx_rtd_theme
make clean
make html
```



## Release Package to PyPI

```bash

# run unit and regression tests
make check

pip install --upgrade build pip twine
rm -rf dist/
python -m build --sdist --wheel -o dist/

# create your ~/.pypirc, if missing
twine upload -r testpypi dist/*
twine upload -r pypi dist/*

```