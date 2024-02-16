#import hashlib
from multiformats import multihash, CID
from functools import lru_cache

def indent(msg: str) -> str:
	ISTR = "  "
	return ISTR + msg.replace("\n", "\n"+ISTR)

@lru_cache(maxsize=1024) # unreasonably effective, lol
def hash_to_cid(data: bytes, codec="dag-cbor") -> CID:
	#digest = b"\x12\x20" + hashlib.sha256(data).digest()
	digest = multihash.digest(data, "sha2-256")
	return CID("base32", 1, codec, digest)
