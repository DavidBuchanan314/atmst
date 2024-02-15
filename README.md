# atmst
A Python library for wrangling atproto-flavoured Merkle Search Trees

Current status: prototype

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
sphinx-apidoc -o source/ ../src/atmst
make html
# open _build/html/index.html
```

publishing to pypi: (this one is mainly for my benefit!)

```
python3 -m build
python3 -m twine upload --repository pypi dist/*
```
