from typing import Self, Optional, Dict, List, Tuple, BinaryIO
from multiformats import varint, CID
import dag_cbor

from . import BlockStore

class ReadOnlyCARBlockStore(BlockStore):
	"""
	This is a sliiiightly unclean abstraction because BlockStores are indexed
	by `bytes` rather than CID, but same idea. This is convenient for verifying
	proofs provided in CAR format, and for testing.
	"""

	car_roots: List[CID]
	block_offsets: Dict[bytes, Tuple[int, int]] # CID -> (offset, length)

	def __init__(self, file: BinaryIO) -> None:
		"""
		pre-scan over the whole file, recording the offsets of each block
		"""

		self.file = file
		file.seek(0)

		# parse out CAR header
		header_len = varint.decode(file)
		header = file.read(header_len)
		if len(header) != header_len:
			raise EOFError("not enough CAR header bytes")
		header_obj = dag_cbor.decode(header)
		if header_obj.get("version") != 1:
			raise ValueError(f"unsupported CAR version ({header_obj.get('version')})")
		self.car_roots = header_obj["roots"]

		# scan through the CAR to find block offsets
		self.block_offsets = {}
		while True:
			try:
				length = varint.decode(file)
			except ValueError:
				break # EOF
			start = file.tell()
			CID_LENGTH = 36  # XXX: this is a questionable assumption!!!
			cid = file.read(CID_LENGTH)
			if cid[:4] != b"\x01\x71\x12\x20": # I think this is enough to verify the assumption
				raise ValueError("unsupported CID type")
			self.block_offsets[cid] = (start + CID_LENGTH, length - CID_LENGTH)
			file.seek(start + length)
	
	def put_block(self, key: bytes, value: bytes) -> None:
		raise NotImplementedError("ReadOnlyCARBlockStore does not support put()")
	
	def get_block(self, key: bytes) -> bytes:
		offset, length = self.block_offsets[key]
		self.file.seek(offset)
		value = self.file.read(length)
		if len(value) != length:
			raise EOFError()
		return value
	
	def del_block(self, key: bytes) -> None:
		raise NotImplementedError("ReadOnlyCARBlockStore does not support delete()")


if __name__ == "__main__":
	f = open("/home/david/programming/python/bskyclient/retr0id.car", "rb")
	bs = ReadOnlyCARBlockStore(f)
	commit_obj = dag_cbor.decode(bs.get_block(bytes(bs.car_roots[0])))
	print(commit_obj)
	mst_root: CID = commit_obj["data"]

	from ..mst import NodeStore
	ns = NodeStore(bs)
	print(ns.get_node(mst_root))
