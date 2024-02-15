import random
from atmst.mst import mst_diff, very_slow_mst_diff, NodeStore, NodeWrangler, hash_to_cid
from atmst.blockstore import MemoryBlockStore
import time

PERF_BENCH = False

def random_test():
	bs = MemoryBlockStore()
	ns = NodeStore(bs)
	nw = NodeWrangler(ns)
	root = ns.get_node(None).cid
	keys = []
	for _ in range(10240 if PERF_BENCH else random.randrange(0, 32)):
		k = random.randbytes(8).hex()
		keys.append(k)
		root = nw.put_record(root, k, hash_to_cid(random.randbytes(8)))
	root_a = root
	for _ in range(8 if PERF_BENCH else random.randrange(0, 8)):
		# some random additions
		root = nw.put_record(root, random.randbytes(8).hex(), hash_to_cid(random.randbytes(8)))
	if keys:
		# some random modifications
		for _ in range(4 if PERF_BENCH else random.randrange(0, 4)):
			for k in random.choice(keys):
				root = nw.put_record(root, k, hash_to_cid(random.randbytes(8)))
		# some random deletions
		for _ in range(4 if PERF_BENCH else random.randrange(0, 4)):
			for k in random.choice(keys):
				root = nw.del_record(root, k)

	diff_start = time.time()
	c, d = mst_diff(ns, root_a, root)
	#c, d = very_slow_mst_diff(ns, root_a, root)
	diff_duration = time.time()-diff_start
	e, f = mst_diff(ns, root, root_a)
	assert(c == f) # compare with reverse
	assert(e == d) # compare with reverse
	g, h = very_slow_mst_diff(ns, root_a, root)
	assert(c == g) # compare with known-good
	assert(d == h) # compare with known-good
	return diff_duration

if __name__ == "__main__":
	duration = 0
	for _ in range(1 if PERF_BENCH else 200):
		duration += random_test()
	print("time spent diffing (ms):", duration*1000)
