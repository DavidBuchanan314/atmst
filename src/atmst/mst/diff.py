from typing import Tuple, Set, Dict, Iterable, Optional
from enum import Enum
from dataclasses import dataclass
import json

from cbrrr import CID

from .node import MSTNode
from .node_store import NodeStore
from .node_walker import NodeWalker

class DeltaType(Enum):
	CREATED = 1
	UPDATED = 2
	DELETED = 3

@dataclass
class RecordDelta:
	delta_type: DeltaType
	path: str
	prior_value: Optional[CID]
	later_value: Optional[CID]

	def __repr__(self) -> str:
		prior = "NULL" if self.prior_value is None else self.prior_value.encode('base32')
		later = "NULL" if self.later_value is None else self.later_value.encode('base32')
		return f"{self.delta_type.name} {json.dumps(self.path)}: {prior} -> {later}"

def record_diff(ns: NodeStore, created: set[CID], deleted: set[CID]) -> Iterable[RecordDelta]:
	"""
	Given two sets of MST nodes (for example, the result of :meth:`mst_diff`), this
	returns an iterator of record changes.
	"""
	created_kv: Dict[str, CID] = dict(sum((list(zip(node.keys, node.vals)) for node in map(ns.get_node, created)), []))
	deleted_kv: Dict[str, CID] = dict(sum((list(zip(node.keys, node.vals)) for node in map(ns.get_node, deleted)), []))
	for created_key in created_kv.keys() - deleted_kv.keys():
		yield RecordDelta(
			delta_type=DeltaType.CREATED,
			path=created_key,
			prior_value=None,
			later_value=created_kv[created_key]
		)
	for updated_key in created_kv.keys() & deleted_kv.keys():
		v1 = deleted_kv[updated_key]
		v2 = created_kv[updated_key]
		if v1 != v2:
			yield RecordDelta(
				delta_type=DeltaType.UPDATED,
				path=updated_key,
				prior_value=v1,
				later_value=v2
			)
	for deleted_key in deleted_kv.keys() - created_kv.keys():
		yield RecordDelta(
			delta_type=DeltaType.DELETED,
			path=deleted_key,
			prior_value=deleted_kv[deleted_key],
			later_value=None
		)

def very_slow_mst_diff(ns: NodeStore, root_a: CID, root_b: CID):
	"""
	This should return the same result as :meth:`mst_diff`, but it gets there in a slow
	but much more obvious way (enumerating all nodes), so it's useful for testing.

	It's actually faster for smaller trees, but it chokes on trees with thousands of nodes (especially if the NodeStore is slow).
	"""
	a_nodes = set(NodeWalker(ns, root_a).iter_node_cids())
	b_nodes = set(NodeWalker(ns, root_b).iter_node_cids())
	return b_nodes - a_nodes, a_nodes - b_nodes

EMPTY_NODE_CID = MSTNode.empty_root().cid

def mst_diff(ns: NodeStore, root_a: CID, root_b: CID) -> Tuple[Set[CID], Set[CID]]: # created, deleted
	"""
	XXX: This implementation is not yet ready for prime-time!

	Given two MST root node CIDs, efficiently compute the difference between the two trees. The result is two sets, holding the created and deleted MST nodes respectively (referenced by CIDs).
	"""
	created = set() # MST nodes in b but not in a
	deleted = set() # MST nodes in a but not in b
	_mst_diff_recursive(created, deleted, NodeWalker(ns, root_a), NodeWalker(ns, root_b))
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

def _mst_diff_recursive(created: Set[CID], deleted: Set[CID], a: NodeWalker, b: NodeWalker): # created, deleted
	# the easiest of all cases
	if a.frame.node == b.frame.node:
		return # no difference
	
	# trivial
	if a.frame.node.is_empty():
		#mst_deleted.add(a.frame.node.cid) # this doesn't work because it might've been a null subtree node
		created |= set(b.iter_node_cids())
		return
	
	# likewise
	if b.frame.node.is_empty():
		#mst_created.add(b.frame.node.cid)
		deleted |= set(a.iter_node_cids())
		return
	
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
	created.add(b.frame.node.cid)
	deleted.add(a.frame.node.cid)

	while True:
		while a.rpath != b.rpath: # we need a loop because they might "leapfrog" each other
			# "catch up" cursor a, if it's behind
			while a.rpath < b.rpath and not a.is_final:
				if a.subtree: # recurse down every subtree
					a.down()
					deleted.add(a.frame.node.cid)
				else:
					a.right()
			
			# catch up cursor b, likewise
			while b.rpath < a.rpath and not b.is_final:
				if b.subtree: # recurse down every subtree
					b.down()
					created.add(b.frame.node.cid)
				else:
					b.right()

		# the rpaths now match, but the subrees below us might not
		
		_mst_diff_recursive(created, deleted, a.subtree_walker(), b.subtree_walker())

		# check if we can still go right XXX: do we need to care about the case where one can, but the other can't?
		# To consider: maybe if I just step a, b will catch up automagically
		if a.rpath == a.stack[0].rpath and b.rpath == b.stack[0].rpath:
			break

		a.right()
		b.right()
