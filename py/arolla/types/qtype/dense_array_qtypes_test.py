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

"""Tests for arolla.types.qtype.dense_array_qtypes."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.operators import operators_clib as _
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import casting
from arolla.types.qtype import dense_array_qtypes
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import scalar_utils_test_helpers
import numpy


class DenseArrayQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          dense_array_qtypes.dense_array_boolean,
          dense_array_qtypes.DENSE_ARRAY_BOOLEAN,
          scalar_utils_test_helpers.BOOLEAN_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_bytes,
          dense_array_qtypes.DENSE_ARRAY_BYTES,
          scalar_utils_test_helpers.BYTES_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_float32,
          dense_array_qtypes.DENSE_ARRAY_FLOAT32,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_float64,
          dense_array_qtypes.DENSE_ARRAY_FLOAT64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_weak_float,
          dense_array_qtypes.DENSE_ARRAY_WEAK_FLOAT,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_int32,
          dense_array_qtypes.DENSE_ARRAY_INT32,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_int64,
          dense_array_qtypes.DENSE_ARRAY_INT64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_uint64,
          dense_array_qtypes.DENSE_ARRAY_UINT64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_text,
          dense_array_qtypes.DENSE_ARRAY_TEXT,
          scalar_utils_test_helpers.TEXT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_unit,
          dense_array_qtypes.DENSE_ARRAY_UNIT,
          scalar_utils_test_helpers.UNIT_TEST_DATA,
      ),
  )
  def test_dense_array_from_values(
      self, dense_array_factory, expected_qtype, test_data
  ):
    values, expected_output = zip(*test_data)
    qvalue = dense_array_factory(values)
    self.assertEqual(qvalue.qtype, expected_qtype)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        list(expected_output),
    )

  def test_dense_array_from_values_generator(self):
    self.assertListEqual(
        array_qtypes.get_array_py_value(
            dense_array_qtypes.dense_array_float32(map(float, range(100)))
        ),
        list(range(100)),
    )

  @parameterized.parameters(
      (dense_array_qtypes.dense_array_unit, [0]),
      (dense_array_qtypes.dense_array_boolean, [0]),
      (dense_array_qtypes.dense_array_boolean, [1]),
      (dense_array_qtypes.dense_array_boolean, [[]]),
      (dense_array_qtypes.dense_array_bytes, ['str']),
      (dense_array_qtypes.dense_array_text, [b'bytes']),
      (dense_array_qtypes.dense_array_int32, ['0']),
      (dense_array_qtypes.dense_array_int64, ['0']),
      (dense_array_qtypes.dense_array_uint64, ['0']),
      (dense_array_qtypes.dense_array_float32, ['0']),
      (dense_array_qtypes.dense_array_float64, ['0']),
      (dense_array_qtypes.dense_array_weak_float, ['0']),
  )
  def test_dense_array_from_values_type_error(
      self, dense_array_factory, values
  ):
    with self.assertRaises(TypeError):
      dense_array_factory(values)

  @parameterized.parameters(
      (
          dense_array_qtypes.dense_array_int32,
          dense_array_qtypes.dense_array_int32,
          dense_array_qtypes.DENSE_ARRAY_INT32,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_int32,
          dense_array_qtypes.dense_array_int64,
          dense_array_qtypes.DENSE_ARRAY_INT64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_int64,
          dense_array_qtypes.dense_array_int64,
          dense_array_qtypes.DENSE_ARRAY_INT64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_weak_float,
          dense_array_qtypes.dense_array_weak_float,
          dense_array_qtypes.DENSE_ARRAY_WEAK_FLOAT,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_weak_float,
          dense_array_qtypes.dense_array_float32,
          dense_array_qtypes.DENSE_ARRAY_FLOAT32,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_weak_float,
          dense_array_qtypes.dense_array_float64,
          dense_array_qtypes.DENSE_ARRAY_FLOAT64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_float32,
          dense_array_qtypes.dense_array_float32,
          dense_array_qtypes.DENSE_ARRAY_FLOAT32,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_float32,
          dense_array_qtypes.dense_array_float64,
          dense_array_qtypes.DENSE_ARRAY_FLOAT64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_float64,
          dense_array_qtypes.dense_array_float64,
          dense_array_qtypes.DENSE_ARRAY_FLOAT64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
  )
  def test_dense_array_from_values_casting(
      self,
      dense_array_factory0,
      dense_array_factory1,
      expected_qtype,
      test_data,
  ):
    values, expected_output = zip(*test_data)
    qvalue0 = dense_array_factory0(values)
    qvalue1 = dense_array_factory1(qvalue0)
    self.assertEqual(qvalue1.qtype, expected_qtype)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue1),
        list(expected_output),
    )

  def test_dense_array_from_values_casting_value_error(self):
    qvalue = dense_array_qtypes.dense_array_text([])
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'failed to convert values with values.qtype=DENSE_ARRAY_TEXT to'
        ' target_qtype=DENSE_ARRAY_FLOAT32 using implicit casting rules',
    ):
      dense_array_qtypes.dense_array_float32(qvalue)

  @parameterized.parameters(
      (
          dense_array_qtypes.dense_array_boolean,
          dense_array_qtypes.DENSE_ARRAY_BOOLEAN,
          numpy.array([False, True]),
      ),
      (
          dense_array_qtypes.dense_array_float32,
          dense_array_qtypes.DENSE_ARRAY_FLOAT32,
          numpy.array([0.5, 1.5, 2.5]),
      ),
      (
          dense_array_qtypes.dense_array_float64,
          dense_array_qtypes.DENSE_ARRAY_FLOAT64,
          numpy.array([0.5, 1.5, 2.5]),
      ),
      (
          dense_array_qtypes.dense_array_weak_float,
          dense_array_qtypes.DENSE_ARRAY_WEAK_FLOAT,
          numpy.array([0.5, 1.5, 2.5]),
      ),
      (
          dense_array_qtypes.dense_array_int32,
          dense_array_qtypes.DENSE_ARRAY_INT32,
          numpy.array([0, 1, 2]),
      ),
      (
          dense_array_qtypes.dense_array_int64,
          dense_array_qtypes.DENSE_ARRAY_INT64,
          numpy.array([0, 1, 2]),
      ),
      (
          dense_array_qtypes.dense_array_uint64,
          dense_array_qtypes.DENSE_ARRAY_UINT64,
          numpy.array([0, 1, 2]),
      ),
  )
  def test_dense_array_from_values_numpy_array(
      self, dense_array_factory, expected_qtype, np_array
  ):
    qvalue = dense_array_factory(np_array)
    self.assertEqual(qvalue.qtype, expected_qtype)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        list(np_array),
    )

  @parameterized.parameters(
      (numpy.bool_, dense_array_qtypes.dense_array_boolean),
      (numpy.int32, dense_array_qtypes.dense_array_int32),
      (numpy.int64, dense_array_qtypes.dense_array_int64),
      (numpy.uint64, dense_array_qtypes.dense_array_uint64),
      (numpy.float32, dense_array_qtypes.dense_array_float32),
      (numpy.float64, dense_array_qtypes.dense_array_float64),
      (numpy.float64, dense_array_qtypes.dense_array_weak_float),
  )
  def test_dense_array_from_values_numpy_array_performance(
      self, dtype, dense_array_factory
  ):
    np_values = (
        numpy.random.RandomState(seed=123).randint(0, 1000, 10**6).astype(dtype)
    )
    _ = dense_array_factory(np_values)

  def test_dense_array_from_values_numpy_array_value_error(self):
    with self.assertRaises(ValueError):
      dense_array_qtypes.dense_array_int32(numpy.array(['foo']))

  def test_dense_array_from_ids_and_values(self):
    values, expected_output = zip(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
    qvalue = dense_array_qtypes.dense_array_float32(
        values=values,
        ids=range(1, len(values) + 1),
        size=len(values) + 2,
    )
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_FLOAT32)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [None] + list(expected_output) + [None],
    )

  def test_dense_array_from_ids_and_values_value_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected arrays of the same sizes')
    ):
      dense_array_qtypes.dense_array_int64(values=[], ids=[0], size=1)
    with self.assertRaisesRegex(
        ValueError, re.escape('expected arrays of the same sizes')
    ):
      dense_array_qtypes.dense_array_int64(values=[0], ids=[], size=1)

  def test_dense_array_from_ids_and_values_type_error(self):
    with self.assertRaisesRegex(
        TypeError, re.escape("'str' object cannot be interpreted as an integer")
    ):
      dense_array_qtypes.dense_array_int64(values=[], ids=[''], size=1)

  @parameterized.parameters(*scalar_utils_test_helpers.BOOLEAN_TEST_DATA)
  def test_dense_array_boolean_from_size_and_value(
      self, value, expected_output
  ):
    qvalue = dense_array_qtypes.dense_array_boolean(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_BOOLEAN)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.BYTES_TEST_DATA)
  def test_dense_array_bytes_from_size_and_value(self, value, expected_output):
    qvalue = dense_array_qtypes.dense_array_bytes(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_BYTES)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_dense_array_float32_from_size_and_value(
      self, value, expected_output
  ):
    qvalue = dense_array_qtypes.dense_array_float32(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_FLOAT32)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_dense_array_float64_from_size_and_value(
      self, value, expected_output
  ):
    qvalue = dense_array_qtypes.dense_array_float64(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_FLOAT64)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_dense_array_weak_float_from_size_and_value(
      self, value, expected_output
  ):
    qvalue = dense_array_qtypes.dense_array_weak_float(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_WEAK_FLOAT)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_dense_array_int32_from_size_and_value(self, value, expected_output):
    qvalue = dense_array_qtypes.dense_array_int32(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_INT32)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_dense_array_int64_from_size_and_value(self, value, expected_output):
    qvalue = dense_array_qtypes.dense_array_int64(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_INT64)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_dense_array_uint64_from_size_and_value(self, value, expected_output):
    qvalue = dense_array_qtypes.dense_array_uint64(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_UINT64)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.TEXT_TEST_DATA)
  def test_dense_array_text_from_size_and_value(self, value, expected_output):
    qvalue = dense_array_qtypes.dense_array_text(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_TEXT)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.UNIT_TEST_DATA)
  def test_dense_array_unit_from_size_and_value(self, value, expected_output):
    qvalue = dense_array_qtypes.dense_array_unit(value, size=5)
    self.assertEqual(qvalue.qtype, dense_array_qtypes.DENSE_ARRAY_UNIT)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(
      *((qtype, True) for qtype in dense_array_qtypes.DENSE_ARRAY_QTYPES),
      *((qtype, False) for qtype in array_qtypes.ARRAY_QTYPES),
      (scalar_qtypes.INT64, False),
  )
  def test_is_dense_array_qtype(self, qtype, expected_value):
    self.assertEqual(
        dense_array_qtypes.is_dense_array_qtype(qtype), expected_value
    )

  def test_is_dense_array_qtype_type_error(self):
    with self.assertRaises(TypeError):
      dense_array_qtypes.is_dense_array_qtype(0)  # pytype: disable=wrong-arg-types

  def test_make_dense_array_qtype(self):
    self.assertEqual(
        dense_array_qtypes.make_dense_array_qtype(scalar_qtypes.INT32),
        dense_array_qtypes.DENSE_ARRAY_INT32,
    )
    self.assertEqual(
        dense_array_qtypes.make_dense_array_qtype(
            optional_qtypes.OPTIONAL_INT32
        ),
        dense_array_qtypes.DENSE_ARRAY_INT32,
    )
    with self.assertRaises(TypeError):
      _ = dense_array_qtypes.make_dense_array_qtype(int)  # pytype: disable=wrong-arg-types
    with self.assertRaises(casting.QTypeError):
      _ = (
          dense_array_qtypes.make_dense_array_qtype(
              dense_array_qtypes.DENSE_ARRAY_INT32
          ),
      )

  def test_dense_array_from_size_and_value_missing_size(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'missing parameter: size'
    ):
      dense_array_qtypes.dense_array_unit(values=[], ids=[])


if __name__ == '__main__':
  absltest.main()
