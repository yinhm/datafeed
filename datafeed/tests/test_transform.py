from __future__ import with_statement

import unittest

from datafeed.transform import int2bytes, bytes2int, b

class TrnasformTest(unittest.TestCase):

    def test_accuracy(self):
        self.assertEqual(int2bytes(123456789), b('\x07[\xcd\x15'))

    def test_codec_identity(self):
        self.assertEqual(bytes2int(int2bytes(123456789, 128)), 123456789)

    def test_chunk_size(self):
        self.assertEqual(int2bytes(123456789, 6), b('\x00\x00\x07[\xcd\x15'))
        self.assertEqual(int2bytes(123456789, 7),
                         b('\x00\x00\x00\x07[\xcd\x15'))

    def test_zero(self):
        self.assertEqual(int2bytes(0, 4), b('\x00') * 4)
        self.assertEqual(int2bytes(0, 7), b('\x00') * 7)
        self.assertEqual(int2bytes(0), b('\x00'))

    def test_correctness_against_base_implementation(self):
        # Slow test.
        values = [
            1 << 512,
            1 << 8192,
            1 << 77,
        ]
        for value in values:
            self.assertEqual(bytes2int(int2bytes(value)), value,
                             "Boom %d" % value)

    def test_raises_OverflowError_when_chunk_size_is_insufficient(self):
        self.assertRaises(OverflowError, int2bytes, 123456789, 3)
        self.assertRaises(OverflowError, int2bytes, 299999999999, 4)

    def test_raises_ValueError_when_negative_integer(self):
        self.assertRaises(ValueError, int2bytes, -1)

    def test_raises_TypeError_when_not_integer(self):
        self.assertRaises(TypeError, int2bytes, None)

if __name__ == '__main__':
    unittest.main()
