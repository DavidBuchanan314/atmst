import hashlib
import dag_cbor
import operator
from multiformats import multihash, CID
from functools import cached_property, reduce
from more_itertools import ilen
from itertools import takewhile
from dataclasses import dataclass
from typing import Tuple, Self, Optional, Any, Dict, List, Set, Type, Iterable
from collections import namedtuple

from util import indent, hash_to_cid
from blockstore import BlockStore, MemoryBlockStore, OverlayBlockStore

# tuple helpers
def tuple_replace_at(original: tuple, i: int, value: Any) -> tuple:
	return original[:i] + (value,) + original[i + 1:]

def tuple_insert_at(original: tuple, i: int, value: Any) -> tuple:
	return original[:i] + (value,) + original[i:]

def tuple_remove_at(original: tuple, i: int) -> tuple:
	return original[:i] + original[i + 1:]


@dataclass(frozen=True) # frozen == immutable == win
class MSTNode:
	"""
	k/v pairs are interleaved between subtrees like so:

	keys:          (0,    1,    2,    3)
	vals:          (0,    1,    2,    3)
	subtrees:   (0,    1,    2,    3,    4)

	If a method is implemented in this class, it's because it's a function/property
	of a single node, as opposed to a whole tree
	"""
	keys: Tuple[str] # collection/rkey
	vals: Tuple[CID] # record CIDs
	subtrees: Tuple[Optional[CID]] # a None value represents an empty subtree


	# NB: __init__ is auto-generated by dataclass decorator

	# these checks should never fail, and could be skipped for performance
	def __post_init__(self) -> None:
		# TODO: maybe check that they're tuples here?
		# implicitly, the length of self.subtrees must be at least 1
		if len(self.subtrees) != len(self.keys) + 1:
			raise ValueError("Invalid subtree count")
		if len(self.keys) != len(self.vals):
			raise ValueError("Mismatched keys/vals lengths")

	@classmethod
	def empty_root(cls) -> Self:
		return cls(
			subtrees=(None,),
			keys=(),
			vals=()
		)

	# this should maybe not be implemented here?
	@staticmethod
	def key_height(key: str) -> int:
		digest = int.from_bytes(hashlib.sha256(key.encode()).digest(), "big")
		leading_zeroes = 256 - digest.bit_length()
		return leading_zeroes // 2

	# since we're immutable, this can be cached
	@cached_property
	def cid(self) -> CID:
		digest = multihash.digest(self.serialised, "sha2-256")
		cid = CID("base32", 1, "dag-cbor", digest)
		return cid

	# likewise
	@cached_property
	def serialised(self) -> bytes:
		e = []
		prev_key = b""
		for subtree, key_str, value in zip(self.subtrees[1:], self.keys, self.vals):
			key_bytes = key_str.encode()
			shared_prefix_len = ilen(takewhile(bool, map(operator.eq, prev_key, key_bytes))) # I love functional programming
			e.append({
				"k": key_bytes[shared_prefix_len:],
				"p": shared_prefix_len,
				"t": subtree,
				"v": value,
			})
			prev_key = key_bytes
		return dag_cbor.encode({
			"e": e,
			"l": self.subtrees[0]
		})

	@classmethod
	def deserialise(cls, data: bytes) -> Self:
		cbor = dag_cbor.decode(data)
		if len(cbor) != 2: # e, l
			raise ValueError("malformed MST node")
		subtrees = [cbor["l"]]
		keys = []
		vals = []
		prev_key = b""
		for e in cbor["e"]: # TODO: make extra sure that these checks are watertight wrt non-canonical representations
			if len(e) != 4: # k, p, t, v
				raise ValueError("malformed MST node")
			prefix_len: int = e["p"]
			suffix: bytes = e["k"]
			if prefix_len > len(prev_key):
				raise ValueError("invalid MST key prefix len")
			if prev_key[prefix_len:prefix_len+1] == suffix[:1]:
				raise ValueError("non-optimal MST key prefix len")
			this_key = prev_key[:prefix_len] + suffix
			if this_key <= prev_key:
				raise ValueError("invalid MST key sort order")
			keys.append(this_key.decode())
			vals.append(e["v"])
			subtrees.append(e["t"])
			prev_key = this_key

		return cls(
			subtrees=tuple(subtrees),
			keys=tuple(keys),
			vals=tuple(vals)
		)
	
	def is_empty(self) -> bool:
		return self.subtrees == (None,)

	def _to_optional(self) -> Optional[CID]:
		"""
		returns None if the node is empty
		"""
		if self.is_empty():
			return None
		return self.cid


	@cached_property
	def height(self) -> int:
		# if there are keys at this level, query one directly
		if self.keys:
			return self.key_height(self.keys[0])
		
		# we're an empty tree
		if self.subtrees[0] is None:
			return 0
		
		# this should only happen for non-root nodes with no keys
		raise Exception("cannot determine node height")
	
	def gte_index(self, key: str) -> int:
		"""
		find the index of the first key greater than or equal to the specified key
		if all keys are smaller, it returns len(keys)
		"""
		i = 0 # this loop could be a binary search but not worth it for small fanouts
		while i < len(self.keys) and key > self.keys[i]:
			i += 1
		return i


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


class NodeWrangler:
	"""
	NodeWrangler is where core MST transformation ops are implemented, backed
	by a NodeStore

	The external APIs take a CID (the MST root) and return a CID (the new root),
	while storing any newly created nodes in the NodeStore.

	Neither method should ever fail - deleting a node that doesn't exist is a nop,
	and adding the same node twice with the same value is also a nop. Callers
	can detect these cases by seeing if the initial and final CIDs changed.
	"""
	ns: NodeStore

	def __init__(self, ns: NodeStore) -> None:
		self.ns = ns

	def put(self, root_cid: CID, key: str, val: CID) -> CID:
		root = self.ns.get_node(root_cid)
		if root.is_empty(): # special case for empty tree
			return self._put_here(root, key, val).cid
		return self._put_recursive(root, key, val, MSTNode.key_height(key), root.height).cid

	def delete(self, root_cid: CID, key: str) -> CID:
		root = self.ns.get_node(root_cid)

		# Note: the seemingly redundant outer .get().cid is required to transform
		# a None cid into the cid representing an empty node (we could maybe find a more elegant
		# way of doing this...)
		return self.ns.get_node(self._squash_top(self._delete_recursive(root, key, MSTNode.key_height(key), root.height))).cid



	def _put_here(self, node: MSTNode, key: str, val: CID) -> MSTNode:
		i = node.gte_index(key)

		# the key is already present!
		if i < len(node.keys) and node.keys[i] == key:
			if node.vals[i] == val:
				return node # we can return our old self if there is no change
			return self.ns.put_node(MSTNode(
				keys=node.keys,
				vals=tuple_replace_at(node.vals, i, val),
				subtrees=node.subtrees
			))
		
		return self.ns.put_node(MSTNode(
			keys=tuple_insert_at(node.keys, i, key),
			vals=tuple_insert_at(node.vals, i, val),
			subtrees = node.subtrees[:i] + \
				self._split_on_key(node.subtrees[i], key) + \
				node.subtrees[i + 1:],
		))
	
	def _put_recursive(self, node: MSTNode, key: str, val: CID, key_height: int, tree_height: int) -> MSTNode:
		if key_height > tree_height: # we need to grow the tree
			return self.ns.put_node(self._put_recursive(
				MSTNode.empty_root(),
				key, val, key_height, tree_height + 1
			))
		
		if key_height < tree_height: # we need to look below
			i = node.gte_index(key)
			return self.ns.put_node(MSTNode(
				keys=node.keys,
				vals=node.vals,
				subtrees=tuple_replace_at(
					node.subtrees, i,
					self._put_recursive(
						self.ns.get_node(node.subtrees[i]),
						key, val, key_height, tree_height - 1
					).cid
				)
			))
		
		# we can insert here
		assert(key_height == tree_height)
		return self._put_here(node, key, val)
	
	def _split_on_key(self, node_cid: Optional[CID], key: str) -> Tuple[Optional[CID], Optional[CID]]:
		if node_cid is None:
			return None, None
		node = self.ns.get_node(node_cid)
		i = node.gte_index(key)
		lsub, rsub = self._split_on_key(node.subtrees[i], key)
		return self.ns.put_node(MSTNode(
			keys=node.keys[:i],
			vals=node.vals[:i],
			subtrees=node.subtrees[:i] + (lsub,)
		))._to_optional(), self.ns.put_node(MSTNode(
			keys=node.keys[i:],
			vals=node.vals[i:],
			subtrees=(rsub,) + node.subtrees[i + 1:],
		))._to_optional()

	def _squash_top(self, node_cid: Optional[CID]) -> Optional[CID]:
		"""
		strip empty nodes from the top of the tree
		"""
		node = self.ns.get_node(node_cid)
		if node.keys:
			return node_cid
		if node.subtrees[0] is None:
			return node_cid
		return self._squash_top(node.subtrees[0])

	def _delete_recursive(self, node: MSTNode, key: str, key_height: int, tree_height: int) -> Optional[CID]:
		if key_height > tree_height: # the key cannot possibly be in this tree, no change needed
			return node._to_optional()
		
		i = node.gte_index(key)
		if key_height < tree_height: # the key must be deleted from a subtree
			if node.subtrees[i] is None:
				return node._to_optional() # the key cannot be in this subtree, no change needed
			return self.ns.put_node(MSTNode(
				keys=node.keys,
				vals=node.vals,
				subtrees=tuple_replace_at(
					node.subtrees,
					i,
					self._delete_recursive(self.ns.get_node(node.subtrees[i]), key, key_height, tree_height - 1)
				)
			))._to_optional()
		
		i = node.gte_index(key)
		if i == len(node.keys) or node.keys[i] != key:
			return node._to_optional() # key already not present
		
		assert(node.keys[i] == key) # sanity check (should always be true)

		return self.ns.put_node(MSTNode(
			keys=tuple_remove_at(node.keys, i),
			vals=tuple_remove_at(node.vals, i),
			subtrees=node.subtrees[:i] + (
				self._merge(node.subtrees[i], node.subtrees[i + 1]),
			) + node.subtrees[i + 2:]
		))._to_optional()
	
	def _merge(self, left_cid: Optional[CID], right_cid: Optional[CID]) -> Optional[CID]:
		if left_cid is None:
			return right_cid # includes the case where left == right == None
		if right_cid is None:
			return left_cid
		left = self.ns.get_node(left_cid)
		right = self.ns.get_node(right_cid)
		return self.ns.put_node(MSTNode(
			keys=left.keys + right.keys,
			vals=left.vals + right.vals,
			subtrees=left.subtrees[:-1] + (
				self._merge(
					left.subtrees[-1],
					right.subtrees[0]
				),
			 ) + right.subtrees[1:]
		))._to_optional()


class NodeWalker:
	"""
	NodeWalker makes implementing tree diffing and other MST query ops more
	convenient (but it does not, itself, implement them).

	A NodeWalker starts off at the root of a tree, and can walk along or recurse
	down into subtrees.

	Walking "off the end" of a subtree brings you back up to its next non-empty parent.

	Recall MSTNode layout:

	keys:  (lkey)  (0,    1,    2,    3)  (rkey)
	vals:          (0,    1,    2,    3)
	subtrees:   (0,    1,    2,    3,    4)
	"""
	KEY_MIN = "" # string that compares less than all legal key strings
	KEY_MAX = "\xff" # string that compares greater than all legal key strings

	@dataclass
	class StackFrame:
		node: MSTNode # could store CIDs only to save memory, in theory, but not much point
		lkey: str
		rkey: str
		idx: int

	ns: NodeStore
	stack: List[StackFrame]
	
	def __init__(self, ns: NodeStore, root_cid: CID, lkey: Optional[str]=KEY_MIN, rkey: Optional[str]=KEY_MAX) -> None:
		self.ns = ns
		self.stack = [self.StackFrame(
			node=self.ns.get_node(root_cid),
			lkey=lkey,
			rkey=rkey,
			idx=0
		)]
	
	def subtree_walker(self) -> Self:
		return NodeWalker(self.ns, self.subtree, self.lkey, self.rkey)
	
	@property
	def frame(self) -> StackFrame:
		return self.stack[-1]

	@property
	def lkey(self) -> str:
		return self.frame.lkey if self.frame.idx == 0 else self.frame.node.keys[self.frame.idx - 1]
	
	@property
	def lval(self) -> Optional[CID]:
		return None if self.frame.idx == 0 else self.frame.node.vals[self.frame.idx - 1]

	@property
	def subtree(self) -> Optional[CID]:
		return self.frame.node.subtrees[self.frame.idx]
	
	# hmmmm rkey is overloaded here... "right key" not "record key"...
	@property
	def rkey(self) -> str:
		return self.frame.rkey if self.frame.idx == len(self.frame.node.keys) else self.frame.node.keys[self.frame.idx]
	
	@property
	def rval(self) -> Optional[CID]:
		return None if self.frame.idx == len(self.frame.node.vals) else self.frame.node.vals[self.frame.idx]

	@property
	def is_final(self) -> bool:
		return (not self.stack) or (self.subtree is None and self.rkey == self.stack[0].rkey)

	def right(self) -> None:
		if (self.frame.idx + 1) >= len(self.frame.node.subtrees):
			# we reached the end of this node, go up a level
			self.stack.pop()
			if not self.stack:
				raise StopIteration # you probably want to check .final instead of hitting this
			return self.right() # we need to recurse, to skip over empty intermediates on the way back up
		self.frame.idx += 1

	def down(self) -> None:
		subtree = self.frame.node.subtrees[self.frame.idx]
		if subtree is None:
			raise Exception("oi, you can't recurse here mate")

		self.stack.append(self.StackFrame(
			node=self.ns.get_node(subtree),
			lkey=self.lkey,
			rkey=self.rkey,
			idx=0
		))
	
	# everything above here is core tree walking logic
	# everything below here is helper functions

	def next_kv(self) -> Tuple[str, CID]:
		while self.subtree: # recurse down every subtree
			self.down()
		self.right()
		return self.lkey, self.lval # the kv pair we just jumped over

	# iterate over every k/v pair in key-sorted order
	def iter_kv(self):
		while not self.is_final:
			yield self.next_kv()
	
	# get all mst nodes down and to the right of the current position
	def iter_node_cids(self):
		yield self.frame.node.cid
		while not self.is_final:
			while self.subtree: # recurse down every subtree
				self.down()
				yield self.frame.node.cid
			self.right()


def enumerate_mst(ns: NodeStore, root_cid: CID):
	for k, v in NodeWalker(ns, root_cid).iter_kv():
		print(k, "->", v.encode("base32"))

# start inclusive, end exclusive
def enumerate_mst_range(ns: NodeStore, root_cid: CID, start: str, end: str):
	cur = NodeWalker(ns, root_cid)
	while True:
		while cur.rkey < start:
			cur.right()
		if not cur.subtree:
			break
		cur.down()

	for k, v, in cur.iter_kv():
		if k >= end:
			break
		print(k, "->", v.encode("base32"))

def record_diff(ns: NodeStore, created: set[CID], deleted: set[CID]):
	created_kv = reduce(operator.__or__, ({ k: v for k, v in zip(node.keys, node.vals)} for node in map(ns.get_node, created)), {})
	deleted_kv = reduce(operator.__or__, ({ k: v for k, v in zip(node.keys, node.vals)} for node in map(ns.get_node, deleted)), {})
	for created_key in created_kv.keys() - deleted_kv.keys():
		yield ("created", created_key, created_kv[created_key].encode("base32"))
	for updated_key in created_kv.keys() & deleted_kv.keys():
		v1 = created_kv[updated_key]
		v2 = deleted_kv[updated_key]
		if v1 != v2:
			yield ("updated", updated_key, v1.encode("base32"), v2.encode("base32"))
	for deleted_key in deleted_kv.keys() - created_kv.keys():
		yield ("deleted", deleted_key, deleted_kv[deleted_key].encode("base32")) #XXX: encode() is just for debugging

EMPTY_NODE_CID = MSTNode.empty_root().cid

def mst_diff(ns: NodeStore, root_a: CID, root_b: CID) -> Tuple[Set[CID], Set[CID]]: # created_deleted
	created, deleted = mst_diff_recursive(NodeWalker(ns, root_a), NodeWalker(ns, root_b))
	middle = created & deleted # my algorithm has occasional false-positives
	#assert(not middle) # this fails
	#print("middle", len(middle))
	created -= middle
	deleted -= middle
	# special case: if one of the root nodes was empty
	if root_a == EMPTY_NODE_CID and root_b != EMPTY_NODE_CID:
		deleted.add(EMPTY_NODE_CID)
	if root_b == EMPTY_NODE_CID and root_a != EMPTY_NODE_CID:
		created.add(EMPTY_NODE_CID)
	return created, deleted

def very_slow_mst_diff(ns, root_a: CID, root_b: CID):
	"""
	This should return the same result as mst_diff, but it gets there in a very slow
	yet less error-prone way, so it's useful for testing.
	"""
	a_nodes = set(NodeWalker(ns, root_a).iter_node_cids())
	b_nodes = set(NodeWalker(ns, root_b).iter_node_cids())
	return b_nodes - a_nodes, a_nodes - b_nodes

def mst_diff_recursive(a: NodeWalker, b: NodeWalker) -> Tuple[Set[CID], Set[CID]]: # created, deleted
	mst_created = set() # MST nodes in b but not in a
	mst_deleted = set() # MST nodes in a but not in b

	# the easiest of all cases
	if a.frame.node.cid == b.frame.node.cid:
		return mst_created, mst_deleted # no difference
	
	# trivial
	if a.frame.node.is_empty():
		mst_created |= set(b.iter_node_cids())
		return mst_created, mst_deleted
	
	# likewise
	if b.frame.node.is_empty():
		mst_deleted |= set(a.iter_node_cids())
		return mst_created, mst_deleted
	
	# now we're onto the hard part

	"""
	theory: most trees that get compared will have lots of shared blocks (which we can skip over, due to identical CIDs)
	completely different trees will inevitably have to visit every node.

	general idea:
	1. if one cursor is "behind" the other, catch it up
	2. when we're matched up, skip over identical subtrees (and recursively diff non-identical subtrees)

	XXX: this seems to work nicely but I'm not sure if it's necessarily efficient for all tree layouts?
	"""

	# NB: these will end up as false-positives if one tree is a subtree of the other
	mst_created.add(b.frame.node.cid)
	mst_deleted.add(a.frame.node.cid)

	while True:
		while a.rkey != b.rkey: # we need a loop because they might "leapfrog" each other
			# "catch up" cursor a, if it's behind
			while a.rkey < b.rkey and not a.is_final:
				if a.subtree: # recurse down every subtree
					a.down()
					mst_deleted.add(a.frame.node.cid)
				else:
					a.right()
			
			# catch up cursor b, likewise
			while b.rkey < a.rkey and not b.is_final:
				if b.subtree: # recurse down every subtree
					b.down()
					mst_created.add(b.frame.node.cid)
				else:
					b.right()

		#print(a.rkey, a.stack[0].rkey, b.rkey, a.stack[0].rkey)
		#assert(b.rkey == a.rkey)
		# the rkeys match, but the subrees below us might not
		
		c, d = mst_diff_recursive(a.subtree_walker(), b.subtree_walker())
		mst_created |= c
		mst_deleted |= d

		# check if we can still go right XXX: do we need to care about the case where one can, but the other can't?
		# To consider: maybe if I just step a, b will catch up automagically
		if a.rkey == a.stack[0].rkey and b.rkey == a.stack[0].rkey:
			break

		a.right()
		b.right()
	
	return mst_created, mst_deleted

if __name__ == "__main__":
	if 0:
		import sys
		sys.setrecursionlimit(999999999)
		from carfile import ReadOnlyCARBlockStore
		f = open("/home/david/programming/python/bskyclient/retr0id.car", "rb")
		bs = OverlayBlockStore(MemoryBlockStore(), ReadOnlyCARBlockStore(f))
		commit_obj = dag_cbor.decode(bs.get_block(bytes(bs.lower.car_roots[0])))
		mst_root: CID = commit_obj["data"]
		ns = NodeStore(bs)
		wrangler = NodeWrangler(ns)
		#print(wrangler)
		#enumerate_mst(ns, mst_root)
		enumerate_mst_range(ns, mst_root, "app.bsky.feed.generator/", "app.bsky.feed.generator/\xff")

		root2 = wrangler.delete(mst_root, "app.bsky.feed.generator/alttext")
		root2 = wrangler.delete(root2, "app.bsky.feed.like/3kas3fyvkti22")
		root2 = wrangler.put(root2, "app.bsky.feed.like/3kc3brpic2z2p", hash_to_cid(b"blah"))

		c, d = mst_diff(ns, mst_root, root2)
		print("CREATED:")
		for x in c:
			print("created", x.encode("base32"))
		print("DELETED:")
		for x in d:
			print("deleted", x.encode("base32"))

		for op in record_diff(ns, c, d):
			print(op)
		
		e, f = very_slow_mst_diff(ns, mst_root, root2)
		assert(e == c)
		assert(f == d)
	else:
		bs = MemoryBlockStore()
		ns = NodeStore(bs)
		wrangler = NodeWrangler(ns)
		root = ns.get_node(None).cid
		print(ns.pretty(root))
		root = wrangler.put(root, "hello", hash_to_cid(b"blah"))
		print(ns.pretty(root))
		root = wrangler.put(root, "foo", hash_to_cid(b"bar"))
		print(ns.pretty(root))
		root_a = root
		root = wrangler.put(root, "bar", hash_to_cid(b"bat"))
		root = wrangler.put(root, "xyzz", hash_to_cid(b"bat"))
		root = wrangler.delete(root, "foo")
		print("=============")
		print(ns.pretty(root_a))
		print("=============")
		print(ns.pretty(root))
		#exit()
		print("=============")
		enumerate_mst(ns, root)
		c, d = mst_diff(ns, root_a, root)
		print("CREATED:")
		for x in c:
			print("created", x.encode("base32"))
		print("DELETED:")
		for x in d:
			print("deleted", x.encode("base32"))
		
		e, f = very_slow_mst_diff(ns, root_a, root)
		assert(e == c)
		assert(f == d)
		
		exit()
		root = wrangler.delete(root, "foo")
		root = wrangler.delete(root, "hello")
		print(ns.pretty(root))
		root = wrangler.delete(root, "bar")
		print(ns.pretty(root))
		root = wrangler.delete(root, "bar")
		print(ns.pretty(root))
