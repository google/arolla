# Copyright 2025 Google LLC
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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import utils

M = arolla.M


@parameterized.named_parameters(*utils.ARRAY_FACTORIES)
class ArrayRandIntLike(parameterized.TestCase):

  def testEmpty(self, array):
    x = arolla.eval(M.array.randint_like(array([], arolla.INT32)))
    self.assertEmpty(x)

  def testSize10(self, array):
    x = arolla.eval(M.array.randint_like(array([None] * 10, arolla.INT32)))
    self.assertLen(x, 10)

  def testReproducibility(self, array):
    a1 = arolla.eval(
        M.array.randint_like(array([None] * 100, arolla.INT32), seed=1)
    )
    b1 = arolla.eval(
        M.array.randint_like(array([None] * 100, arolla.INT32), seed=1)
    )
    c2 = arolla.eval(
        M.array.randint_like(array([None] * 100, arolla.INT32), seed=2)
    )
    self.assertEqual(a1.fingerprint, b1.fingerprint)
    self.assertNotEqual(a1.fingerprint, c2.fingerprint)

  def testNoNegativesByDefault(self, array):
    x = arolla.eval(M.array.randint_like(array([None] * 100, arolla.INT32)))
    self.assertGreaterEqual(min(x), 0)

  def testValueRange(self, array):
    x = arolla.eval(
        M.array.randint_like(
            array([None] * 1000, arolla.INT32), low=-10, high=20, seed=100
        )
    )
    self.assertEqual(min(x), -10)
    self.assertEqual(max(x), 19)
    self.assertLen(set(x), 30)

  def testValueArrayRange(self, array):
    with self.subTest('both_arrays'):
      x = arolla.eval(
          M.array.randint_like(
              array([None] * 1000, arolla.INT32),
              low=array([-10] * 1000, arolla.INT64),
              high=array([20] * 1000, arolla.INT64),
              seed=100
          )
      )
      self.assertEqual(min(x), -10)
      self.assertEqual(max(x), 19)
      self.assertLen(set(x), 30)

    with self.subTest('high_array'):
      x = arolla.eval(
          M.array.randint_like(
              array([None] * 1000, arolla.INT32),
              low=-10,
              high=array([20] * 1000, arolla.INT64),
              seed=100
          )
      )
      self.assertEqual(min(x), -10)
      self.assertEqual(max(x), 19)
      self.assertLen(set(x), 30)

    with self.subTest('low_array'):
      x = arolla.eval(
          M.array.randint_like(
              array([None] * 1000, arolla.INT32),
              low=array([-10] * 1000, arolla.INT64),
              high=20,
              seed=100
          )
      )
      self.assertEqual(min(x), -10)
      self.assertEqual(max(x), 19)
      self.assertLen(set(x), 30)

  def testValueArrayWithMissing(self, array):
    x = arolla.eval(
        M.array.randint_like(
            array([None, None, None], arolla.INT32),
            low=array([0, None, 5], arolla.INT64),
            high=array([10, 20, None], arolla.INT64),
            seed=100
        )
    )
    self.assertGreaterEqual(x[0].py_value(), 0)
    self.assertLess(x[0].py_value(), 10)
    self.assertIsNone(x[1].py_value())
    self.assertIsNone(x[2].py_value())

  def testTrivialValueRange(self, array):
    x = arolla.eval(
        M.array.randint_like(array([None] * 100, arolla.INT32), low=1, high=2)
    )
    self.assertEqual(set(x), {1})

  def testEmptyValueRange(self, array):
    with self.assertRaises(ValueError):
      arolla.eval(
          M.array.randint_like(array([None] * 100, arolla.INT32), high=-1)
      )

    with self.assertRaises(ValueError):
      arolla.eval(
          M.array.randint_like(
              array([None] * 100, arolla.INT32), high=array([-1] * 100)
          )
      )


if __name__ == '__main__':
  absltest.main()
