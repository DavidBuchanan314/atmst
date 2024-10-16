# atmst

[![Static Badge](https://img.shields.io/badge/license-MIT-blue)](./LICENSE) ![Static Badge](https://img.shields.io/badge/works%20on%20my%20machine-forestgreen) ![Static Badge](https://img.shields.io/badge/test%20coverage-nonzero-red) [![Static Badge](https://img.shields.io/badge/docs-exist!-orange)](https://davidbuchanan314.github.io/atmst/) ![Static Badge](https://img.shields.io/badge/cryptography-certified%20hand--rolled-yellow)

A Python library for wrangling atproto-flavoured Merkle Search Trees

Current status: ⚠️ prototype ⚠️

### Installation

```
git clone https://github.com/DavidBuchanan314/atmst
cd atmst
python3 -m pip install .
```

dev install: (editable)

```
python3 -m pip install -e .
```

Running the tests:

```
python3 -m unittest discover -v
```

build the docs:

```
cd docs/
sphinx-apidoc -f -o _apidocs/ ../src/atmst ../src/atmst/all.py
make html
# open _build/html/index.html
```

publishing to pypi: (this one is mainly for my benefit!)

```
python3 -m build
python3 -m twine upload --repository pypi dist/*
```
