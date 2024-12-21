from typing import Set, Tuple, Optional

from cbrrr import CID

from .node import MSTNode
from .node_store import NodeStore
from .node_walker import NodeWalker

# validating a proof failed
class InvalidProof(ValueError):
	pass

# constructing a proof failed
class ProofError(ValueError):
	pass

# works for both inclusion and exclusion proofs
def find_rpath_and_build_proof(ns: NodeStore, root_cid: CID, rpath: str) -> Tuple[Optional[CID], Set[CID]]:
	walker = NodeWalker(ns, root_cid)
	value = walker.find_rpath(rpath) # returns None if not found
	proof = {frame.node.cid for frame in walker.stack}
	return value, proof

def build_exclusion_proof(ns: NodeStore, root_cid: CID, rpath: str) -> Set[CID]:
	value, proof = find_rpath_and_build_proof(ns, root_cid, rpath)
	if value is not None:
		raise ProofError("can't build exclusion proof for a record that exists!")
	return proof

def build_inclusion_proof(ns: NodeStore, root_cid: CID, rpath: str) -> Set[CID]:
	value, proof = find_rpath_and_build_proof(ns, root_cid, rpath)
	if value is None:
		raise ProofError("can't build inclusion proof for a record that doesn't exist!")
	return proof

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
