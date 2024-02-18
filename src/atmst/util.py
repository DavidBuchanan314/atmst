import hashlib
from cbrrr import CID

def indent(msg: str) -> str:
	ISTR = "  "
	return ISTR + msg.replace("\n", "\n"+ISTR)

def hash_to_cid(data: bytes, codec="dag-cbor") -> CID:
	digest = b"\x12\x20" + hashlib.sha256(data).digest()
	return CID((b"\x00\x01q" if codec == "dag-cbor" else b'\x00\x01U') + digest) # XXX: this is a hack!
