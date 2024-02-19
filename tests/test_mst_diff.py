import unittest

from atmst.all import MemoryBlockStore, NodeStore, NodeWrangler, mst_diff, very_slow_mst_diff
from atmst.mst.node import MSTNode
from cbrrr import CID

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

if __name__ == '__main__':
	unittest.main(module="tests.test_mst_diff")
