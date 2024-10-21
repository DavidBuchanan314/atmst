import unittest
import random

from atmst.all import MemoryBlockStore, NodeStore, NodeWrangler, mst_diff, very_slow_mst_diff
from atmst.mst.node import MSTNode
from cbrrr import CID

def dump_mst(ns: NodeStore, cid: CID, lvl=0):
	node = ns.get_node(cid)
	print("  "*lvl + "-", node)
	for subtree in node.subtrees:
		if subtree:
			dump_mst(ns, subtree, lvl+1)

class MSTDiffTestCase(unittest.TestCase):
	def setUp(self):
		keys = []
		dummy_value = CID.cidv1_dag_cbor_sha256_32_from(b"value")
		i = 0
		for height in [0, 1, 0, 2, 0, 1, 0]: # if all these keys are added to a MST, it'll form a perfect binary tree.
			while True:
				key = f"{i:04d}"
				i += 1
				if MSTNode.key_height(key) == height:
					keys.append(key)
					break
		
		bs = MemoryBlockStore()
		self.ns = NodeStore(bs)
		wrangler = NodeWrangler(self.ns)

		# create all possible permutations of the full binary tree
		# the idea is that this'll cover most "interesting" trees up to a height of 3

		self.trees = []
		for i in range(2**len(keys)):
			root = self.ns.get_node(None).cid
			for j, k in enumerate(keys):
				if (i>>j)&1:
					root = wrangler.put_record(root, k, dummy_value)
			self.trees.append(root)
	
	def test_diff_all_pairs(self):
		for a in self.trees:
			for b in self.trees:
				reference_created, reference_deleted = very_slow_mst_diff(self.ns, a, b)
				created, deleted = mst_diff(self.ns, a, b)
				self.assertEqual(created, reference_created)
				self.assertEqual(deleted, reference_deleted)
	
	def test_insertion_order_independent(self):
		wrangler = NodeWrangler(self.ns)

		keys = [str(x) for x in range(1000)]

		mst_a = MSTNode.empty_root().cid
		mst_b = MSTNode.empty_root().cid
		mst_c = MSTNode.empty_root().cid

		for k in keys:
			mst_a = wrangler.put_record(mst_a, k, CID.cidv1_dag_cbor_sha256_32_from(k.encode()))

		for k in keys[::-1]:
			mst_b = wrangler.put_record(mst_b, k, CID.cidv1_dag_cbor_sha256_32_from(k.encode()))

		random.shuffle(keys)
		for k in keys:
			mst_c = wrangler.put_record(mst_c, k, CID.cidv1_dag_cbor_sha256_32_from(k.encode()))

		#print()
		#dump_mst(self.ns, mst_a)

		#print()
		#dump_mst(self.ns, mst_b)

		self.assertEqual(mst_a, mst_b)
		self.assertEqual(mst_a, mst_c)

if __name__ == '__main__':
	unittest.main(module="tests.test_mst_diff")
