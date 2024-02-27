import unittest
import io

from atmst.blockstore.car_file import decode_varint, encode_varint

class MSTDiffTestCase(unittest.TestCase):
	def test_varint_encode(self):
		self.assertEqual(encode_varint(0), b"\x00")
		self.assertEqual(encode_varint(1), b"\x01")
		self.assertEqual(encode_varint(127), b"\x7f")
		self.assertEqual(encode_varint(128), b"\x80\x01")
		self.assertEqual(encode_varint(2**63-1), b'\xff\xff\xff\xff\xff\xff\xff\xff\x7f')
		self.assertRaises(ValueError, encode_varint, 2**63)
		self.assertRaises(ValueError, encode_varint, -1)

	def test_varint_decode(self):
		self.assertEqual(decode_varint(io.BytesIO(b"\x00")), 0)
		self.assertEqual(decode_varint(io.BytesIO(b"\x01")), 1)
		self.assertEqual(decode_varint(io.BytesIO(b"\x7f")), 127)
		self.assertEqual(decode_varint(io.BytesIO(b"\x80\x01")), 128)
		self.assertEqual(decode_varint(io.BytesIO(b'\xff\xff\xff\xff\xff\xff\xff\xff\x7f')), 2**63-1)
		self.assertRaises(ValueError, decode_varint, io.BytesIO(b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\x7f')) # too big
		self.assertRaises(ValueError, decode_varint, io.BytesIO(b"")) # too short
		self.assertRaises(ValueError, decode_varint, io.BytesIO(b'\xff')) # truncated
		self.assertRaises(ValueError, decode_varint, io.BytesIO(b"\x80\x00")) # not minimally encoded

if __name__ == '__main__':
	unittest.main(module="tests.test_varint")
