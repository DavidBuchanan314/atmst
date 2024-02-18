.. _overview:

Library Overview
================

If you have some `atproto repository <https://atproto.com/specs/repository>`_ data, and you want to operate on it with Python, you've come to the right place [1]_. The APIs offered here are rather low-level, but I'm planning on adding higher-level helper utilities in the future.

.. [1] Maybe also check out `arroba <https://github.com/snarfed/arroba>`_!

=============
Block Storage
=============

The foundations of repos are content-addressed Blocks of data, as in the `IPLD <https://ipld.io/docs/motivation/benefits-of-content-addressing/>`_ data model. The abstract :meth:`~atmst.blockstore.BlockStore` interface facilitates access to blocks, agnostic of the underlying storage medium. The following implementations are available:

* :meth:`~atmst.blockstore.MemoryBlockStore` - stores blocks in memory only (inside a dict)

* :meth:`~atmst.blockstore.car_file.ReadOnlyCARBlockStore` - accesses the contents of a CAR file.

* :meth:`~atmst.blockstore.SqliteBlockStore` - accesses blocks stored in a table of an sqlite database.

Finally, the :meth:`~atmst.blockstore.OverlayBlockStore` class allows you to layer one BlockStore over another, with writes going to the top layer only. This is useful in several scenarios, for example, reading blocks from two CAR files at once so that you can diff them, or for staging modifications in memory ready to be committed to persistent storage.

===================
Merkle Search Trees
===================

With a BlockStore, we can read and write content-addressed blocks of data. Content-addressing is cool, but sometimes you want mutability. The `Merkle Search Tree <https://inria.hal.science/hal-02303490/document>`_ data structure builds on top of content-addressed Block storage, providing a mutable map of keys onto values. In atproto, the keys are arbitrary strings (under certain constraints), and the values are "records".

Everything is still immutable under the hood, so modifying an MST results in a new root hash.

:py:mod:`atmst` doesn't have a dedicated class to represent an MST (yet?), instead we just reference the root node by CID.

=====
Nodes
=====

An MST is comprised of one or more Nodes. :py:mod:`atmst` represents Nodes using :meth:`~atmst.mst.node.MSTNode`, an immutable dataclass.

Nodes are ultimately stored in a BlockStore, serialised as `DAG-CBOR <https://ipld.io/docs/codecs/known/dag-cbor/>`_, and the :meth:`~atmst.mst.node_store.NodeStore` class facilitates this. A NodeStore also maintains an LRU cache, mapping CIDs to MSTNode objects, to reduce the impact of BlockStore read latency, hash verification, and deserialisation overheads.

The :meth:`~atmst.mst.node_wrangler.NodeWrangler` class facilitates modifications to MSTs, and the :meth:`~atmst.mst.node_walker.NodeWalker` class facilitates access to MSTs, which the :meth:`~atmst.mst.diff.mst_diff` method makes use of.
