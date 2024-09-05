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

"""Tests for numpy_conversion."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.experimental import numpy_conversion
import numpy as np


@parameterized.parameters(
    (arolla.INT32, np.int32),
    (arolla.INT64, np.int64),
    (arolla.types.UINT64, np.uint64),
    (arolla.FLOAT32, np.float32),
    (arolla.FLOAT64, np.float64),
    (arolla.WEAK_FLOAT, np.float64),
)
class NumericConversionTest(parameterized.TestCase):

  def test_assert_type_and_shape(self, arolla_qtype, np_dtype):
    converted = numpy_conversion.as_numpy_array(
        arolla.array([1, 2, 3], value_qtype=arolla_qtype)
    )
    self.assertIsInstance(converted, np.ndarray)
    self.assertEqual(converted.dtype, np_dtype)
    self.assertEqual(converted.shape, (3,))

  def test_assert_converts(self, arolla_qtype, np_dtype):
    converted = numpy_conversion.as_numpy_array(
        arolla.array([1, 2, 3], value_qtype=arolla_qtype)
    )
    self.assertSameElements(converted, np.array([1, 2, 3], dtype=np_dtype))

  def test_assert_converts_filling_value(self, arolla_qtype, np_dtype):
    converted = numpy_conversion.as_numpy_array(
        arolla.array([0, None, 1], value_qtype=arolla_qtype), fill_value=128
    )

    self.assertSameElements(converted, np.array([0, 128, 1], dtype=np_dtype))

  def test_assert_converts_no_present_values(self, arolla_qtype, np_dtype):
    converted = numpy_conversion.as_numpy_array(
        arolla.array([None, None, None], value_qtype=arolla_qtype),
        fill_value=128,
    )

    self.assertSameElements(
        converted, np.array([128, 128, 128], dtype=np_dtype)
    )

  def test_conversion_back_works(self, arolla_qtype, _):
    array = arolla.array([1, 2, 3], value_qtype=arolla_qtype)
    converted = numpy_conversion.as_numpy_array(array)
    converted_back = arolla.array(converted, value_qtype=arolla_qtype)
    self.assertSameElements(converted_back, array)


@parameterized.parameters(
    (arolla.FLOAT32, np.float32),
    (arolla.FLOAT64, np.float64),
    (arolla.WEAK_FLOAT, np.float64),
)
class FloatDefaultFillValueConversionTest(parameterized.TestCase):

  def test_assert_converts_filling_nans(self, arolla_qtype, _):
    converted = numpy_conversion.as_numpy_array(
        arolla.array([0, None, 1], value_qtype=arolla_qtype)
    )

    self.assertEqual(converted[0], 0)
    self.assertTrue(np.isnan(converted[1]))
    self.assertEqual(converted[2], 1)


@parameterized.parameters(
    (arolla.INT32, np.int32),
    (arolla.INT64, np.int64),
    (arolla.types.UINT64, np.uint64),
)
class IntDefaultFillValueConversionTest(parameterized.TestCase):

  def test_assert_converts_filling_zeros(self, arolla_qtype, np_dtype):
    converted = numpy_conversion.as_numpy_array(
        arolla.array([0, None, 1], value_qtype=arolla_qtype)
    )

    self.assertSameElements(converted, np.array([0, 0, 1], dtype=np_dtype))


@parameterized.parameters(
    (
        arolla.array_unit([True, None, True]),
        np.array([True, False, True]),
    ),
    (
        arolla.array_boolean([False, None, True]),
        np.array([False, None, True], dtype=object),
    ),
    (
        arolla.array_text(['a', 'b', None, 'c']),
        np.array(['a', 'b', None, 'c'], dtype=object),
    ),
    (
        arolla.array_bytes([b'a', b'b', None, b'c']),
        np.array([b'a', b'b', None, b'c'], dtype=object),
    ),
)
class NonNumericConversionTest(parameterized.TestCase):

  def test_assert_type_and_shape(self, arolla_array, expected_result):
    converted = numpy_conversion.as_numpy_array(arolla_array)
    self.assertIsInstance(converted, np.ndarray)
    self.assertEqual(converted.dtype, expected_result.dtype)
    self.assertEqual(converted.shape, (arolla_array.size,))

  def test_assert_converts(self, arolla_array, expected_result):
    converted = numpy_conversion.as_numpy_array(arolla_array)
    self.assertSameElements(converted, expected_result)

  def test_assert_converts_fill_value_throws_error(self, arolla_array, _):
    with self.assertRaises(TypeError):
      numpy_conversion.as_numpy_array(arolla_array, fill_value=-1)


class TriboolConversionTest(parameterized.TestCase):

  def test_as_numpy_array_tribool_no_missing_values(self):
    array = arolla.array_boolean([True, False, None])
    result = numpy_conversion.as_numpy_array_tribool(array)
    self.assertSameElements(result, [1, -1, 0])


class AsNumpyArrayWithIdsTest(parameterized.TestCase):

  def test_as_numpy_array_with_indices_no_missing_values(self):
    array = arolla.array([5, 11, 27], value_qtype=arolla.INT32)
    indices, values = numpy_conversion.as_numpy_array_with_indices(array)
    self.assertSameElements(indices, [0, 1, 2])
    self.assertSameElements(values, [5, 11, 27])

  def test_as_numpy_array_with_indices_has_missing_values(self):
    array = arolla.array([5, None, 27], value_qtype=arolla.INT32)
    indices, values = numpy_conversion.as_numpy_array_with_indices(array)
    self.assertSameElements(indices, [0, 2])
    self.assertSameElements(values, [5, 27])

  def test_as_numpy_array_with_indices_no_present_values(self):
    array = arolla.array([None, None, None], value_qtype=arolla.INT32)
    indices, values = numpy_conversion.as_numpy_array_with_indices(array)
    self.assertEmpty(indices)
    self.assertEmpty(values)


class PresentIndicesAsNumpyArrayTest(parameterized.TestCase):

  def test_present_indices_as_numpy_array_no_missing_values(self):
    array = arolla.array([5, 11, 27], value_qtype=arolla.INT32)
    indices = numpy_conversion.present_indices_as_numpy_array(array)
    self.assertSameElements(indices, [0, 1, 2])

  def test_present_indices_as_numpy_array_has_missing_values(self):
    array = arolla.array([5, None, 27], value_qtype=arolla.INT32)
    indices = numpy_conversion.present_indices_as_numpy_array(array)
    self.assertSameElements(indices, [0, 2])

  def test_present_indices_as_numpy_array_no_present_values(self):
    array = arolla.array([None, None, None], value_qtype=arolla.INT32)
    indices = numpy_conversion.present_indices_as_numpy_array(array)
    self.assertEmpty(indices)


class PresentValuesAsNumpyArrayTest(parameterized.TestCase):

  def test_present_values_as_numpy_array_no_missing_values(self):
    array = arolla.array([5, 11, 27], value_qtype=arolla.INT32)
    values = numpy_conversion.present_values_as_numpy_array(array)
    self.assertSameElements(values, [5, 11, 27])

  def test_present_values_as_numpy_array_has_missing_values(self):
    array = arolla.array([5, None, 27], value_qtype=arolla.INT32)
    values = numpy_conversion.present_values_as_numpy_array(array)
    self.assertSameElements(values, [5, 27])

  def test_present_values_as_numpy_array_no_present_values(self):
    array = arolla.array([None, None, None], value_qtype=arolla.INT32)
    values = numpy_conversion.present_values_as_numpy_array(array)
    self.assertEmpty(values)


if __name__ == '__main__':
  absltest.main()
