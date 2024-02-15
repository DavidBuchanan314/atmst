from typing import Optional, Dict

from multiformats import CID

from ..blockstore import BlockStore
from ..util import indent
from .node import MSTNode

class NodeStore:
	"""
	NodeStore wraps a BlockStore to provide a more ergonomic interface
	for loading and storing MSTNodes
	"""
	bs: BlockStore
	cache: Dict[Optional[CID], MSTNode] # XXX: this cache will grow forever!
	#cache_counts: Dict[Optional[CID], int]

	def __init__(self, bs: BlockStore) -> None:
		self.bs = bs
		self.cache = {}
		#self.cache_counts = {}
	
	# TODO: LRU cache this - this package looks ideal: https://github.com/amitdev/lru-dict
	def get_node(self, cid: Optional[CID]) -> MSTNode:
		cached = self.cache.get(cid)
		if cached:
			return cached
		"""
		if cid is None, returns an empty MST node
		"""
		if cid is None:
			return self.put_node(MSTNode.empty_root())
		
		res = MSTNode.deserialise(self.bs.get_block(bytes(cid)))
		self.cache[cid] = res
		return res
	
	# TODO: also put in cache
	def put_node(self, node: MSTNode) -> MSTNode:
		self.cache[node.cid] = node
		self.bs.put_block(bytes(node.cid), node.serialised)
		return node # this is convenient

	# MST pretty-printing
	# this should maybe not be implemented here
	def pretty(self, node_cid: Optional[CID]) -> str:
		if node_cid is None:
			return "<empty>"
		node = self.get_node(node_cid)
		res = f"MSTNode<cid={node.cid.encode("base32")}>(\n{indent(self.pretty(node.subtrees[0]))},\n"
		for k, v, t in zip(node.keys, node.vals, node.subtrees[1:]):
			res += f"  {k!r} ({MSTNode.key_height(k)}) -> {v.encode("base32")},\n"
			res += indent(self.pretty(t)) + ",\n"
		res += ")"
		return res
