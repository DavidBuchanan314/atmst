import random
from atmst import MemoryBlockStore, NodeStore, NodeWrangler
from atmst.util import hash_to_cid
import time

dummy_cid = hash_to_cid(b"hello")

def insert_random(n: int):
	bs = MemoryBlockStore()
	ns = NodeStore(bs)
	nw = NodeWrangler(ns)
	root = ns.get_node(None).cid
	nw.put_record(root, "0", dummy_cid)
	nw.put_record(root, "\xff", dummy_cid)
	for _ in range(n):
		root = nw.put_record(root, random.randbytes(8).hex(), dummy_cid)

def insert_sequential(n: int):
	bs = MemoryBlockStore()
	ns = NodeStore(bs)
	nw = NodeWrangler(ns)
	root = ns.get_node(None).cid
	nw.put_record(root, "0", dummy_cid)
	nw.put_record(root, "\xff", dummy_cid)
	for i in range(n):
		root = nw.put_record(root, f"{i:08d}", dummy_cid)

if __name__ == "__main__":
	start = time.time()
	insert_random(1000)
	print("insert random", time.time() - start)

	start = time.time()
	insert_sequential(1000)
	print("insert sequential", time.time() - start)
