import random
from atmst.all import MemoryBlockStore, NodeStore, NodeWrangler
from cbrrr import CID
import time

dummy_cid = CID.cidv1_dag_cbor_sha256_32_from(b"hello")

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
	insert_random(10000)
	print("insert random", time.time() - start)

	start = time.time()
	insert_sequential(10000)
	print("insert sequential", time.time() - start)
