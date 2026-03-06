from typing import Dict, List, Tuple, BinaryIO
import hashlib

from cbrrr import decode_dag_cbor, CID

from . import BlockStore

# should be equivalent to multiformats.varint.decode(), but not extremely slow for no reason.
def decode_varint(stream: BinaryIO):
	n = 0
	for shift in range(0, 63, 7):
		val = stream.read(1)
		if not val:
			raise ValueError("unexpected end of varint input")
		val = val[0]
		n |= (val & 0x7f) << shift
		if not val & 0x80:
			if shift and not val:
				raise ValueError("varint not minimally encoded")
			return n
		shift += 7
	raise ValueError("varint too long")

def encode_varint(n: int) -> bytes:
	if not 0 <= n < 2**63:
		raise ValueError("integer out of encodable varint range")
	res = []
	while n > 0x7f:
		res.append(0x80 | (n & 0x7f))
		n >>= 7
	res.append(n)
	return bytes(res)

class ReadOnlyCARBlockStore(BlockStore):
	"""
	This is a sliiiightly unclean abstraction because BlockStores are indexed
	by `bytes` rather than CID, but same idea. This is convenient for verifying
	proofs provided in CAR format, and for testing.
	"""

	car_root: CID
	block_offsets: Dict[bytes, Tuple[int, int]] # CID -> (offset, length)

	def __init__(self, file: BinaryIO, validate_hashes: bool=True) -> None:
		"""
		pre-scan over the whole file, recording the offsets of each block
		"""

		self.file = file
		self.validate_hashes = validate_hashes
		file.seek(0)

		# parse out CAR header
		header_len = decode_varint(file)
		header = file.read(header_len)
		if len(header) != header_len:
			raise EOFError("not enough CAR header bytes")
		header_obj = decode_dag_cbor(header)
		if not isinstance(header_obj, dict):
			raise TypeError
		if header_obj.get("version") != 1:
			raise ValueError(f"unsupported CAR version ({header_obj.get('version')})")
		roots = header_obj["roots"]
		if not isinstance(roots, list):
			raise TypeError
		if len(roots) != 1:
			raise ValueError(f"unsupported number of CAR roots ({len(roots)}, expected 1)")
		root = roots[0]
		if not isinstance(root, CID):
			raise TypeError
		self.car_root = root

		# scan through the CAR to find block offsets
		self.block_offsets = {}
		while True:
			try:
				length = decode_varint(file)
			except ValueError:
				break # EOF
			start = file.tell()
			CID_LENGTH = 36  # XXX: this is a questionable assumption!!!
			cid = CID(file.read(CID_LENGTH))
			if not cid.is_cidv1_dag_cbor_sha256_32(): # I think this is enough to verify the assumption
				raise ValueError("unsupported CID type")
			self.block_offsets[bytes(cid)] = (start + CID_LENGTH, length - CID_LENGTH)
			file.seek(start + length)
	
	def put_block(self, key: bytes, value: bytes) -> None:
		raise NotImplementedError("ReadOnlyCARBlockStore does not support put()")
	
	def get_block(self, key: bytes) -> bytes:
		offset, length = self.block_offsets[key]
		self.file.seek(offset)
		value = self.file.read(length)
		if len(value) != length:
			raise EOFError()
		if self.validate_hashes:
			if key[:4] != CID.CIDV1_DAG_CBOR_SHA256_32_PFX:
				raise ValueError("unsupported CID type")
			digest = hashlib.sha256(value).digest()
			if digest != key[4:]:
				raise ValueError("bad CID hash!")
		return value
	
	def del_block(self, key: bytes) -> None:
		raise NotImplementedError("ReadOnlyCARBlockStore does not support delete()")

class OptimisticRetryError(KeyError):
	"""Raised when an optimistic streaming read fails and a retry with
	optimistic=False may succeed."""
	pass

class OpportunisticStreamingCarBlockStore(BlockStore):
	"""
	A BlockStore backed by a CarStreamReader. Optimistically reads blocks
	sequentially from the stream, assuming preorder traversal order. If a
	block is received out of order, falls back to slurping the rest of
	the stream into memory.

	This should work in 99.999% of cases. The fallback case could fail if the CAR
	starts off in canonical order, but has duplicate record CIDs where the second
	record is *not* duplicated into the CAR. Duplicate CIDs are relatively common
	in-the-wild, but accidentally-canonical block order is rare.

	In such a case, an OptimisticRetryError is raised, and the caller should
	retry from scratch with optimistic=False.
	"""

	car_root: CID

	def __init__(self, carstream: "CarStreamReader", optimistic: bool=True) -> None:
		self.car_root = carstream.car_root
		self._car_iter = iter(carstream)
		self._blocks: Dict[bytes, bytes] = {}
		self._optimistic = optimistic
		self._was_optimistic = optimistic
		if not optimistic:
			self._slurp(self._car_iter)

	def _slurp(self, car_iter) -> None:
		for k, v in car_iter:
			self._blocks[bytes(k)] = v

	def get_block(self, key: bytes) -> bytes:
		if self._optimistic:
			try:
				k, v = next(self._car_iter)
			except StopIteration:
				raise KeyError(f"block not found: {key!r}")
			if bytes(k) == key:
				return v
			# CAR is not canonically ordered, slurp the rest into memory
			self._optimistic = False
			self._blocks[bytes(k)] = v
			self._slurp(self._car_iter)
			# fall thru

		try:
			return self._blocks[key]
		except KeyError:
			if self._was_optimistic:
				raise OptimisticRetryError(key)
			raise

	def is_canonical(self) -> bool:
		if not self._optimistic:
			return False
		# check that the stream has no remaining blocks
		try:
			next(self._car_iter)
			return False
		except StopIteration:
			return True

	def put_block(self, key: bytes, value: bytes) -> None:
		raise NotImplementedError

	def del_block(self, key: bytes) -> None:
		raise NotImplementedError


class CarStreamReader:
	"""
	Rather than pre-indexing the block offsets, this lets you iterate over the k/v pairs
	"""
	car_root: CID
	def __init__(self, file: BinaryIO, validate_hashes: bool=True) -> None:
		self.file = file
		self.validate_hashes = validate_hashes

		# parse out CAR header
		header_len = decode_varint(file)
		header = file.read(header_len)
		if len(header) != header_len:
			raise EOFError("not enough CAR header bytes")
		header_obj = decode_dag_cbor(header)
		if not isinstance(header_obj, dict):
			raise TypeError
		if header_obj.get("version") != 1:
			raise ValueError(f"unsupported CAR version ({header_obj.get('version')})")
		roots = header_obj["roots"]
		if not isinstance(roots, list):
			raise TypeError
		if len(roots) != 1:
			raise ValueError(f"unsupported number of CAR roots ({len(roots)}, expected 1)")
		root = roots[0]
		if not isinstance(root, CID):
			raise TypeError
		self.car_root = root
	
	def __iter__(self) -> "CarStreamReader":
		return self

	def __next__(self):
		try:
			length = decode_varint(self.file)
		except ValueError:
			raise StopIteration
		
		CID_LENGTH = 36  # XXX: this is a questionable assumption!!!
		cid = CID(self.file.read(CID_LENGTH))
		if not cid.is_cidv1_dag_cbor_sha256_32(): # I think this is enough to verify the assumption
			raise ValueError("unsupported CID type")

		value = self.file.read(length - CID_LENGTH)

		if CID_LENGTH + len(value) != length:
			raise ValueError("unexpected read length")

		if self.validate_hashes:
			digest = hashlib.sha256(value).digest()
			if digest != cid.cid_bytes[4:]: # XXX: again, assumes is_cidv1_dag_cbor_sha256_32
				raise ValueError("bad CID hash!")

		return cid, value
