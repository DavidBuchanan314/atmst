from typing import Optional, Dict
from functools import lru_cache

from cbrrr import CID
from lru import LRU

from ..blockstore import BlockStore
from ..util import indent
from .node import MSTNode

class NodeStore:
	"""
	NodeStore wraps a BlockStore to provide a more ergonomic interface
	for loading and storing MSTNodes
	"""
	bs: BlockStore
	cache: Dict[Optional[CID], MSTNode]

	def __init__(self, bs: BlockStore) -> None:
		self.bs = bs
		self.cache = LRU(1024)
	
	def get_node(self, cid: Optional[CID]) -> MSTNode:
		if cached := self.cache.get(cid): # look in our LRU cache first
			return cached
		"""
		if cid is None, returns an empty MST node
		"""
		if cid is None:
			return self.stored_node(MSTNode.empty_root())
		
		node_bytes = self.bs.get_block(bytes(cid))
		node = MSTNode.deserialise(node_bytes)

		# prime the cached_properties since we already know their values
		object.__setattr__(node, "serialised", node_bytes)
		object.__setattr__(node, "cid", cid)

		# prime the node cache
		self.cache[cid] = node

		return node
	
	def stored_node(self, node: MSTNode) -> MSTNode:
		self.cache[node.cid] = node # also put it in the LRU cache
		self.bs.put_block(bytes(node.cid), node.serialised)
		return node # this is convenient

	# MST pretty-printing
	# this should maybe not be implemented here
	def pretty(self, node_cid: Optional[CID]) -> str:
		if node_cid is None:
			return "<empty>"
		node = self.get_node(node_cid)
		res = f"MSTNode<cid={node.cid.encode('base32')}>(\n{indent(self.pretty(node.subtrees[0]))},\n"
		for k, v, t in zip(node.keys, node.vals, node.subtrees[1:]):
			res += f"  {k!r} ({MSTNode.key_height(k)}) -> {v.encode('base32')},\n"
			res += indent(self.pretty(t)) + ",\n"
		res += ")"
		return res
