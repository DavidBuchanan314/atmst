from dataclasses import dataclass
from typing import Tuple, Self, Optional, List

from multiformats import CID

from . import MSTNode
from .node_store import NodeStore

class NodeWalker:
	"""
	NodeWalker makes implementing tree diffing and other MST query ops more
	convenient (but it does not, itself, implement them).

	A NodeWalker starts off at the root of a tree, and can walk along or recurse
	down into subtrees.

	Walking "off the end" of a subtree brings you back up to its next non-empty parent.

	Recall MSTNode layout: ::

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
