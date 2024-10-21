from typing import Tuple, Optional, Any

from cbrrr import CID

from .node import MSTNode
from .node_store import NodeStore

# tuple helpers
def _tuple_replace_at(original: tuple, i: int, value: Any) -> tuple:
	return original[:i] + (value,) + original[i + 1:]

def _tuple_insert_at(original: tuple, i: int, value: Any) -> tuple:
	return original[:i] + (value,) + original[i:]

def _tuple_remove_at(original: tuple, i: int) -> tuple:
	return original[:i] + original[i + 1:]


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

	def put_record(self, root_cid: CID, key: str, val: CID) -> CID:
		root = self.ns.get_node(root_cid)
		if root.is_empty(): # special case for empty tree
			return self._put_here(root, key, val).cid
		return self._put_recursive(root, key, val, MSTNode.key_height(key), root.height).cid

	def del_record(self, root_cid: CID, key: str) -> CID:
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
			return self.ns.stored_node(MSTNode(
				keys=node.keys,
				vals=_tuple_replace_at(node.vals, i, val),
				subtrees=node.subtrees
			))
		
		return self.ns.stored_node(MSTNode(
			keys=_tuple_insert_at(node.keys, i, key),
			vals=_tuple_insert_at(node.vals, i, val),
			subtrees = node.subtrees[:i] + \
				self._split_on_key(node.subtrees[i], key) + \
				node.subtrees[i + 1:],
		))
	
	def _put_recursive(self, node: MSTNode, key: str, val: CID, key_height: int, tree_height: int) -> MSTNode:
		if key_height > tree_height: # we need to grow the tree
			return self.ns.stored_node(self._put_recursive(
				self.ns.stored_node(MSTNode(
					keys=(),
					vals=(),
					subtrees=(node.cid,)
				)),
				key, val, key_height, tree_height + 1
			))
		
		if key_height < tree_height: # we need to look below
			i = node.gte_index(key)
			return self.ns.stored_node(MSTNode(
				keys=node.keys,
				vals=node.vals,
				subtrees=_tuple_replace_at(
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
		return self.ns.stored_node(MSTNode(
			keys=node.keys[:i],
			vals=node.vals[:i],
			subtrees=node.subtrees[:i] + (lsub,)
		))._to_optional(), self.ns.stored_node(MSTNode(
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
			return self.ns.stored_node(MSTNode(
				keys=node.keys,
				vals=node.vals,
				subtrees=_tuple_replace_at(
					node.subtrees,
					i,
					self._delete_recursive(self.ns.get_node(node.subtrees[i]), key, key_height, tree_height - 1)
				)
			))._to_optional()
		
		i = node.gte_index(key)
		if i == len(node.keys) or node.keys[i] != key:
			return node._to_optional() # key already not present
		
		assert(node.keys[i] == key) # sanity check (should always be true)

		return self.ns.stored_node(MSTNode(
			keys=_tuple_remove_at(node.keys, i),
			vals=_tuple_remove_at(node.vals, i),
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
		return self.ns.stored_node(MSTNode(
			keys=left.keys + right.keys,
			vals=left.vals + right.vals,
			subtrees=left.subtrees[:-1] + (
				self._merge(
					left.subtrees[-1],
					right.subtrees[0]
				),
			 ) + right.subtrees[1:]
		))._to_optional()
