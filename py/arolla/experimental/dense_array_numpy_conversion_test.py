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

"""Tests for dense_array_conversion."""

import gc
import weakref

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.experimental import dense_array_numpy_conversion
import numpy as np


class DenseArrayConversionTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          arolla.dense_array_boolean,
          [True, False, True, False],
          'bool_',
      ),
      (
          arolla.dense_array_float32,
          [0.0, 1.5, 2.0, -1.5, -float('inf'), float('inf')],
          'float32',
      ),
      (
          arolla.dense_array_float64,
          [0.0, 1.5, 2.0, -1.5, -float('inf'), float('inf')],
          'float64',
      ),
      (
          arolla.dense_array_weak_float,
          [0.0, 1.5, 2.0, -1.5, -float('inf'), float('inf')],
          'float64',
      ),
      (
          arolla.dense_array_int32,
          [1, 2, -1, 2**31 - 1, -(2**31)],
          'int32',
      ),
      (
          arolla.dense_array_int64,
          [1, 2, -1, 2**63 - 1, -(2**63)],
          'int64',
      ),
      (
          arolla.types.dense_array_uint64,
          [0, 1, 2, (2**64) - 1],
          'uint64',
      ),
  )
  def test_numpy_ndarray_from_dense_array(
      self, dense_array_factory, py_values, expected_dtype_name
  ):
    dense_array = dense_array_factory(py_values)
    array = dense_array_numpy_conversion.numpy_ndarray_from_dense_array(
        dense_array
    )
    self.assertEqual(array.dtype, expected_dtype_name)
    np.testing.assert_array_equal(array, py_values)
    # Ensure that the data's lifetime is not tied to the lifetime of
    # the dense_array.
    dense_array_weakref = weakref.ref(dense_array)
    del dense_array
    gc.collect()
    self.assertIsNone(dense_array_weakref())
    # By now, we know that the dense_array qvalue instance is gone.
    np.testing.assert_array_equal(array, py_values)

  def test_numpy_ndarray_from_dense_array_value_error(self):
    dense_array = arolla.dense_array_float32([None, 1.5])
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'dense array has missing elements, cannot provide a memoryview',
    ):
      dense_array_numpy_conversion.numpy_ndarray_from_dense_array(dense_array)

  def test_numpy_ndarray_from_dense_array_not_implemeneted_error(self):
    dense_array = arolla.dense_array_bytes([b'foo'])
    with self.assertRaisesWithLiteralMatch(
        NotImplementedError,
        'cannot provide a memoryview (qtype=DENSE_ARRAY_BYTES)',
    ):
      dense_array_numpy_conversion.numpy_ndarray_from_dense_array(dense_array)


if __name__ == '__main__':
  absltest.main()
