# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for M.core.zip."""

from absl.testing import absltest
from arolla import arolla

L = arolla.L
M = arolla.M


class CoreZipTest(absltest.TestCase):

  def testTupleLiteralArgs(self):
    self.assertEqual(
        arolla.eval(M.core.zip((0.0, 1, b'2'), (3.0, 4, b'5'))).py_value(),
        ((0.0, 3.0), (1, 4), (b'2', b'5')),
    )

  def testMakeTupleOpArgs(self):
    x1 = M.core.make_tuple(
        arolla.float32(0.0), arolla.int32(1), arolla.bytes(b'2')
    )
    x2 = M.core.make_tuple(
        arolla.float32(3.0), arolla.int32(4), arolla.bytes(b'5')
    )
    self.assertEqual(
        arolla.eval(M.core.zip(x1, x2)).py_value(),
        ((0.0, 3.0), (1, 4), (b'2', b'5')),
    )

  def testNoArgs(self):
    self.assertEqual(arolla.eval(M.core.zip()).py_value(), ())
    self.assertEqual(arolla.eval(M.core.zip((), ())).py_value(), ())

  def testSizeMismatchError(self):
    with self.assertRaisesRegex(
        ValueError, r'all tuple arguments must be of the same size'
    ):
      M.core.zip((1, 2, 3), (4, 5))

  def testErrorNonTupleInt(self):
    with self.assertRaisesRegex(ValueError, 'non-tuple object with no fields'):
      arolla.eval(M.core.zip(1, 2))

  def testErrorNonTupleMix(self):
    with self.assertRaisesRegex(ValueError, 'non-tuple object with no fields'):
      t = M.core.make_tuple(arolla.float32(0.0), arolla.float32(1.0))
      arolla.eval(M.core.zip(t, L.x), x=arolla.dense_array_float32([1.0, 2.0]))


if __name__ == '__main__':
  absltest.main()
