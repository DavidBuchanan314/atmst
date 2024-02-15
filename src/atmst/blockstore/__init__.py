from abc import ABC, abstractmethod
from typing import Optional, Dict
import sqlite3


class BlockStore(ABC):
	"""
	A block store is a k/v store where values are immutable once set. They can be deleted, though.
	In practice, k==hash(v), but this API doesn't care about that.

	I'm not using the "native" __getitem__, __setitem__, __del__ methods because
	the semantics of these methods differ subtly.

	if you call put() twice with the same args, the second call is a nop.
	if you call put() twice with the same key but different value, you get a ValueError

	get() offers no default return value, you get a KeyError if it doesn't exist.

	if you try to delete a key that doesn't exist, that's a nop.
	"""

	@abstractmethod
	def put_block(self, key: bytes, value: bytes) -> None:
		pass

	@abstractmethod
	def get_block(self, key: bytes) -> bytes:
		pass

	@abstractmethod
	def del_block(self, key: bytes) -> None:
		pass


class MemoryBlockStore(BlockStore):
	_state = Dict[bytes, bytes]

	def __init__(self, state: Optional[Dict[bytes, bytes]]=None) -> None:
		"""
		NB: if a state dict is passed, it'll get mutated in-place
		"""
		self._state = dict() if state is None else state
	
	def put_block(self, key: bytes, value: bytes) -> None:
		existing_value = self._state.get(key)
		if existing_value:
			if existing_value == value:
				return  # the value matches, there's nothing to do
			raise ValueError("block values are immutable")
		self._state[key] = value
	
	def get_block(self, key: bytes) -> bytes:
		value = self._state.get(key)
		if value is None:
			raise KeyError("no block matches this key")
		return value
	
	def del_block(self, key: bytes) -> None:
		if key in self._state:
			del self._state[key]


class SqliteBlockStore(BlockStore):
	"""
	NB: Caller is responsible for calling commit(), etc.
	TODO: consider allowing a custom table name?
	"""
	def __init__(self, con: sqlite3.Connection, table: str="mst_blocks") -> None:
		self.table = table
		self._cur = con.cursor()
		self._cur.execute(f"""
			CREATE TABLE IF NOT EXISTS {self.table} (
				block_key BLOB PRIMARY KEY,
				block_val BLOB NOT NULL
			) WITHOUT ROWID;
		""")
	
	def put_block(self, key: bytes, value: bytes) -> None:
		# XXX: this will fail silently if the key already exists but with a different value
		# (that should never happen but it'd be nice to have guard rails)
		self._cur.execute(f"INSERT OR IGNORE INTO {self.table} (block_key, block_val) VALUES (?, ?)", (key, value))
	
	def get_block(self, key: bytes) -> bytes:
		row = self._cur.execute(f"SELECT block_val FROM {self.table} WHERE block_key=?", (key,)).fetchone()
		if row is None:
			raise KeyError("no block matches this key")
		return row[0]
	
	def del_block(self, key: bytes) -> None:
		self._cur.execute(f"DELETE FROM {self.table} WHERE block_key=?", (key,))


class OverlayBlockStore(BlockStore):
	"""
	reads come from "upper", then "lower" if they don't exist in upper.
	writes/deletes go only to "upper".
	"""

	def __init__(self, upper: BlockStore, lower: BlockStore) -> None:
		self.upper = upper
		self.lower = lower
	
	def put_block(self, key: bytes, value: bytes) -> None:
		self.upper.put_block(key, value)
	
	def get_block(self, key: bytes) -> bytes:
		try:
			return self.upper.get_block(key)
		except KeyError:
			return self.lower.get_block(key)
	
	def del_block(self, key: bytes) -> None:
		self.upper.del_block(key)


"""
if __name__ == "__main__":
	import os

	bs = MemoryBlockStore()
	bs.put_block(b"hello", b"world")

	bs.put_block(b"hello", b"world") # putting twice is a nop

	try:
		bs.put_block(b"hello", b"foobar")
		assert(False) # should be unreachable
	except ValueError:
		pass

	print("hello ->", bs.get_block(b"hello"))

	bs.del_block(b"nothing") # nop

	bs.del_block(b"hello")

	try:
		bs.get_block(b"hello")
		assert(False) # should be unreachable
	except KeyError:
		pass

	TEST_DB = "test.db"

	with sqlite3.connect(TEST_DB) as db:
		bs = SqliteBlockStore(db)
		bs.put_block(b"hello", b"sqlite world")

	with sqlite3.connect(TEST_DB) as db:
		bs = SqliteBlockStore(db)
		print("hello ->", bs.get_block(b"hello"))
		bs.del_block(b"hello")
	
	try:
		with sqlite3.connect(TEST_DB) as db:
			bs = SqliteBlockStore(db)
			print("hello ->", bs.get_block(b"hello"))
		assert(False) # should be unreachable
	except KeyError:
		pass

	os.remove(TEST_DB) # clean up
"""
