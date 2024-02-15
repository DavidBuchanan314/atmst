# atmst

![Static Badge](https://img.shields.io/badge/license-MIT-blue) ![Static Badge](https://img.shields.io/badge/works%20on%20my%20machine-green) ![Static Badge](https://img.shields.io/badge/test%20coverage-0%25-red) ![Static Badge](https://img.shields.io/badge/docs-maybe%20one%20day-red) ![Static Badge](https://img.shields.io/badge/cryptography-certified%20hand--rolled-yellow) 

A Python library for wrangling atproto-flavoured Merkle Search Trees

Current status: ⚠️ prototype ⚠️

### Installation

```
git clone https://github.com/DavidBuchanan314/atmst
cd atmst
python3 -m pip install .
```

dev install:

```
python3 -m pip install -e .
```

build the docs:

```
cd docs/
sphinx-apidoc -f -o _apidocs/ ../src/atmst # not sure if this is needed every time
make html
# open _build/html/index.html
```

publishing to pypi: (this one is mainly for my benefit!)

```
python3 -m build
python3 -m twine upload --repository pypi dist/*
```
