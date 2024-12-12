from typing import Set, Tuple, Optional

from cbrrr import CID

from .node import MSTNode
from .node_store import NodeStore
from .node_walker import NodeWalker

class InvalidProof(Exception):
	pass

# works for both inclusion and exclusion proofs
def find_rpath_and_build_proof(ns: NodeStore, root_cid: CID, rpath: str) -> Tuple[Optional[CID], Set[CID]]:
	walker = NodeWalker(ns, root_cid)
	value = walker.find_rpath(rpath) # returns None if not found
	proof = {frame.node.cid for frame in walker.stack}
	return value, proof

def verify_inclusion(ns: NodeStore, root_cid: CID, rpath: str) -> None:
	walker = NodeWalker(ns, root_cid)
	try:
		if walker.find_rpath(rpath) is None:
			raise InvalidProof("rpath not present in MST")
	except KeyError:
		raise InvalidProof("missing MST blocks")

def verify_exclusion(ns: NodeStore, root_cid: CID, rpath: str) -> None:
	walker = NodeWalker(ns, root_cid)
	try:
		if walker.find_rpath(rpath) is not None:
			raise InvalidProof("rpath *is* present in MST")
	except KeyError:
		raise InvalidProof("missing MST blocks")
