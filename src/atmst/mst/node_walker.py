from dataclasses import dataclass
from typing import TYPE_CHECKING, Tuple, Optional, List, Iterable
if TYPE_CHECKING: # Self doesn't exist <3.11
	from typing import Self

from cbrrr import CID

from .node import MSTNode
from .node_store import NodeStore

class NodeWalker:
	"""
	NodeWalker makes implementing tree diffing and other MST query ops more
	convenient (but it does not, itself, implement them).

	A NodeWalker starts off at the root of a tree, and can walk along or recurse
	down into subtrees.

	Walking "off the end" of a subtree brings you back up to its next non-empty parent.

	Recall MSTNode layout: ::

		keys:  (lpath)  (0,    1,    2,    3)  (rpath)
		vals:           (0,    1,    2,    3)
		subtrees:    (0,    1,    2,    3,    4)

	"""
	PATH_MIN = "" # string that compares less than all legal path strings
	PATH_MAX = "\xff" # string that compares greater than all legal path strings

	@dataclass
	class StackFrame:
		node: MSTNode # could store CIDs only to save memory, in theory, but not much point
		lpath: str
		rpath: str
		idx: int

	ns: NodeStore
	stack: List[StackFrame]
	
	def __init__(self, ns: NodeStore, root_cid: Optional[CID], lpath: Optional[str]=PATH_MIN, rpath: Optional[str]=PATH_MAX) -> None:
		self.ns = ns
		self.stack = [self.StackFrame(
			node=MSTNode.empty_root() if root_cid is None else self.ns.get_node(root_cid),
			lpath=lpath,
			rpath=rpath,
			idx=0
		)]
	
	def subtree_walker(self) -> "Self":
		return NodeWalker(self.ns, self.subtree, self.lpath, self.rpath)
	
	@property
	def frame(self) -> StackFrame:
		return self.stack[-1]

	@property
	def height(self) -> int:
		return self.frame.node.height

	@property
	def lpath(self) -> str:
		return self.frame.lpath if self.frame.idx == 0 else self.frame.node.keys[self.frame.idx - 1]
	
	@property
	def lval(self) -> Optional[CID]:
		return None if self.frame.idx == 0 else self.frame.node.vals[self.frame.idx - 1]

	@property
	def subtree(self) -> Optional[CID]:
		return self.frame.node.subtrees[self.frame.idx]
	
	@property
	def rpath(self) -> str:
		return self.frame.rpath if self.frame.idx == len(self.frame.node.keys) else self.frame.node.keys[self.frame.idx]
	
	@property
	def rval(self) -> Optional[CID]:
		return None if self.frame.idx == len(self.frame.node.vals) else self.frame.node.vals[self.frame.idx]

	@property
	def is_final(self) -> bool:
		# is (not self.stack) really necesasry here? is that a reachable state?
		return (not self.stack) or (self.subtree is None and self.rpath == self.stack[0].rpath)

	@property
	def can_go_right(self) -> bool:
		return (self.frame.idx + 1) < len(self.frame.node.subtrees)

	def right_or_up(self) -> None:
		if not self.can_go_right:
			# we reached the end of this node, go up a level
			self.stack.pop()
			if not self.stack:
				raise StopIteration # you probably want to check .final instead of hitting this
			return self.right_or_up() # we need to recurse, to skip over empty intermediates on the way back up
		self.frame.idx += 1

	def right(self) -> None:
		if not self.can_go_right:
			raise Exception("cursor is already at rightmost position in node")
		self.frame.idx += 1

	def down(self) -> None:
		subtree = self.frame.node.subtrees[self.frame.idx]
		if subtree is None:
			raise Exception("oi, you can't recurse here mate (subtree is None)")

		self.stack.append(self.StackFrame(
			node=self.ns.get_node(subtree),
			lpath=self.lpath,
			rpath=self.rpath,
			idx=0
		))
	
	# everything above here is core tree walking logic
	# everything below here is helper functions

	def next_kv(self) -> Tuple[str, CID]:
		while self.subtree: # recurse down every subtree
			self.down()
		self.right_or_up()
		return self.lpath, self.lval # the kv pair we just jumped over

	# iterate over every k/v pair in key-sorted order
	# NB: should really be p/v standing for path/value
	def iter_kv(self) -> Iterable[Tuple[str, CID]]:
		while not self.is_final:
			yield self.next_kv()
	
	# get all mst nodes down and to the right of the current position
	def iter_nodes(self) -> Iterable[MSTNode]:
		yield self.frame.node
		while not self.is_final:
			while self.subtree: # recurse down every subtree
				self.down()
				yield self.frame.node
			self.right_or_up()

	def iter_node_cids(self) -> Iterable[CID]:
		for node in self.iter_nodes():
			yield node.cid

	# start inclusive
	def iter_kv_range(self, start: str, end: str, end_inclusive: bool=False) -> Iterable[Tuple[str, CID]]:
		while True:
			while self.rpath < start:
				self.right_or_up()
			if not self.subtree:
				break
			self.down()

		for k, v, in self.iter_kv():
			if k > end or (not end_inclusive and k == end):
				break
			yield k, v

	# TODO: we need to make this early-exit so that it can work with concise deletion proofs, maybe
	# (early exit based on key height - might need significant rewrite)
	def find_rpath(self, rpath: str) -> Optional[CID]:
		rpath_height = MSTNode.key_height(rpath)
		while True:
			# if the rpath we're looking for is higher than the current cursor,
			# we're never going to find it (i.e. we early-exit)
			if rpath_height > self.height:
				return None
			while self.rpath < rpath: # either look for the rpath, or the right point to go down
				if not self.can_go_right:
					return None
				self.right()
			if self.rpath == rpath:
				return self.rval # found it!
			if not self.subtree:
				return None # need to go down, but we can't
			self.down()
