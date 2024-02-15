from multiformats import multihash, CID

def indent(msg: str) -> str:
	ISTR = "  "
	return ISTR + msg.replace("\n", "\n"+ISTR)

def hash_to_cid(data: bytes, codec="dag-cbor") -> CID:
	digest = multihash.digest(data, "sha2-256")
	return CID("base32", 1, codec, digest)
