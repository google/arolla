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

import itertools
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import casting
from arolla.types.qtype import dense_array_qtypes
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import scalar_utils
from arolla.types.qtype import scalar_utils_test_helpers
import numpy


class ArrayQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          array_qtypes.array_boolean,
          array_qtypes.ARRAY_BOOLEAN,
          scalar_utils_test_helpers.BOOLEAN_TEST_DATA,
      ),
      (
          array_qtypes.array_bytes,
          array_qtypes.ARRAY_BYTES,
          scalar_utils_test_helpers.BYTES_TEST_DATA,
      ),
      (
          array_qtypes.array_float32,
          array_qtypes.ARRAY_FLOAT32,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_float64,
          array_qtypes.ARRAY_FLOAT64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_weak_float,
          array_qtypes.ARRAY_WEAK_FLOAT,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_int32,
          array_qtypes.ARRAY_INT32,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_int64,
          array_qtypes.ARRAY_INT64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_uint64,
          array_qtypes.ARRAY_UINT64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_text,
          array_qtypes.ARRAY_TEXT,
          scalar_utils_test_helpers.TEXT_TEST_DATA,
      ),
      (
          array_qtypes.array_unit,
          array_qtypes.ARRAY_UNIT,
          scalar_utils_test_helpers.UNIT_TEST_DATA,
      ),
  )
  def test_array_from_values(self, array_factory, expected_qtype, test_data):
    values, expected_output = zip(*test_data)
    qvalue = array_factory(values)
    self.assertEqual(qvalue.qtype, expected_qtype)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        list(expected_output),
    )

  def test_array_from_values_generator(self):
    self.assertListEqual(
        array_qtypes.get_array_py_value(
            array_qtypes.array_float32(map(float, range(100)))
        ),
        list(range(100)),
    )

  @parameterized.parameters(
      (array_qtypes.array_unit, [0]),
      (array_qtypes.array_boolean, [0]),
      (array_qtypes.array_boolean, [1]),
      (array_qtypes.array_boolean, [[]]),
      (array_qtypes.array_bytes, ['str']),
      (array_qtypes.array_text, [b'bytes']),
      (array_qtypes.array_int32, ['0']),
      (array_qtypes.array_int64, ['0']),
      (array_qtypes.array_uint64, ['0']),
      (array_qtypes.array_float32, ['0']),
      (array_qtypes.array_float64, ['0']),
      (array_qtypes.array_weak_float, ['0']),
  )
  def test_array_from_values_type_error(self, array_factory, values):
    with self.assertRaises(TypeError):
      array_factory(values)

  @parameterized.parameters(
      (
          array_qtypes.array_int32,
          array_qtypes.array_int32,
          array_qtypes.ARRAY_INT32,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_int32,
          array_qtypes.array_int64,
          array_qtypes.ARRAY_INT64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_int64,
          array_qtypes.array_int64,
          array_qtypes.ARRAY_INT64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_weak_float,
          array_qtypes.array_weak_float,
          array_qtypes.ARRAY_WEAK_FLOAT,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_weak_float,
          array_qtypes.array_float32,
          array_qtypes.ARRAY_FLOAT32,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_weak_float,
          array_qtypes.array_float64,
          array_qtypes.ARRAY_FLOAT64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_float32,
          array_qtypes.array_float32,
          array_qtypes.ARRAY_FLOAT32,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_float32,
          array_qtypes.array_float64,
          array_qtypes.ARRAY_FLOAT64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_float64,
          array_qtypes.array_float64,
          array_qtypes.ARRAY_FLOAT64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
  )
  def test_array_from_values_casting(
      self,
      array_factory0,
      array_factory1,
      expected_qtype,
      test_data,
  ):
    values, expected_output = zip(*test_data)
    qvalue0 = array_factory0(values)
    qvalue1 = array_factory1(qvalue0)
    self.assertEqual(qvalue1.qtype, expected_qtype)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue1),
        list(expected_output),
    )

  def test_array_from_values_casting_value_error(self):
    qvalue = array_qtypes.array_text([])
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'failed to convert values with values.qtype=ARRAY_TEXT to'
        ' target_qtype=ARRAY_FLOAT32 using implicit casting rules',
    ):
      array_qtypes.array_float32(qvalue)

  @parameterized.parameters(
      (
          array_qtypes.array_boolean,
          array_qtypes.ARRAY_BOOLEAN,
          numpy.array([False, True]),
      ),
      (
          array_qtypes.array_float32,
          array_qtypes.ARRAY_FLOAT32,
          numpy.array([0.5, 1.5, 2.5]),
      ),
      (
          array_qtypes.array_float64,
          array_qtypes.ARRAY_FLOAT64,
          numpy.array([0.5, 1.5, 2.5]),
      ),
      (
          array_qtypes.array_weak_float,
          array_qtypes.ARRAY_WEAK_FLOAT,
          numpy.array([0.5, 1.5, 2.5]),
      ),
      (
          array_qtypes.array_int32,
          array_qtypes.ARRAY_INT32,
          numpy.array([0, 1, 2]),
      ),
      (
          array_qtypes.array_int64,
          array_qtypes.ARRAY_INT64,
          numpy.array([0, 1, 2]),
      ),
      (
          array_qtypes.array_uint64,
          array_qtypes.ARRAY_UINT64,
          numpy.array([0, 1, 2]),
      ),
  )
  def test_array_from_values_numpy_array(
      self, array_factory, expected_qtype, np_array
  ):
    qvalue = array_factory(np_array)
    self.assertEqual(qvalue.qtype, expected_qtype)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        list(np_array),
    )

  @parameterized.parameters(
      (numpy.bool_, array_qtypes.array_boolean),
      (numpy.int32, array_qtypes.array_int32),
      (numpy.int64, array_qtypes.array_int64),
      (numpy.uint64, array_qtypes.array_uint64),
      (numpy.float32, array_qtypes.array_float32),
      (numpy.float64, array_qtypes.array_float64),
      (numpy.float64, array_qtypes.array_weak_float),
  )
  def test_array_from_values_numpy_array_performance(
      self, dtype, array_factory
  ):
    np_values = (
        numpy.random.RandomState(seed=123).randint(0, 1000, 10**6).astype(dtype)
    )
    _ = array_factory(np_values)

  def test_array_from_values_numpy_array_value_error(self):
    with self.assertRaises(ValueError):
      array_qtypes.array_int32(numpy.array(['foo']))

  def test_array_from_ids_and_values(self):
    values, expected_output = zip(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
    qvalue = array_qtypes.array_float32(
        values=values,
        ids=range(1, len(values) + 1),
        size=len(values) + 2,
    )
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_FLOAT32)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [None] + list(expected_output) + [None],
    )

  def test_array_from_ids_and_values_value_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected arrays of the same sizes')
    ):
      array_qtypes.array_int64(values=[], ids=[0], size=1)
    with self.assertRaisesRegex(
        ValueError, re.escape('expected arrays of the same sizes')
    ):
      array_qtypes.array_int64(values=[0], ids=[], size=1)

  def test_array_from_ids_and_values_type_error(self):
    with self.assertRaisesRegex(
        TypeError, re.escape("'str' object cannot be interpreted as an integer")
    ):
      array_qtypes.array_int64(values=[], ids=[''], size=1)

  @parameterized.parameters(*scalar_utils_test_helpers.BOOLEAN_TEST_DATA)
  def test_array_boolean_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_boolean(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_BOOLEAN)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.BYTES_TEST_DATA)
  def test_array_bytes_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_bytes(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_BYTES)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_array_float32_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_float32(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_FLOAT32)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_array_float64_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_float64(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_FLOAT64)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_array_weak_float_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_weak_float(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_WEAK_FLOAT)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_array_int32_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_int32(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_INT32)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_array_int64_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_int64(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_INT64)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_array_uint64_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_uint64(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_UINT64)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.TEXT_TEST_DATA)
  def test_array_text_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_text(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_TEXT)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(*scalar_utils_test_helpers.UNIT_TEST_DATA)
  def test_array_unit_from_size_and_value(self, value, expected_output):
    qvalue = array_qtypes.array_unit(value, size=5)
    self.assertEqual(qvalue.qtype, array_qtypes.ARRAY_UNIT)
    self.assertListEqual(
        array_qtypes.get_array_py_value(qvalue),
        [expected_output] * 5,
    )

  @parameterized.parameters(
      *((qtype, True) for qtype in array_qtypes.ARRAY_QTYPES),
      *((qtype, True) for qtype in dense_array_qtypes.DENSE_ARRAY_QTYPES),
      (scalar_qtypes.INT64, False),
  )
  def test_is_array_qtype(self, qtype, expected_value):
    self.assertEqual(array_qtypes.is_array_qtype(qtype), expected_value)

  def test_is_array_qtype_type_error(self):
    with self.assertRaises(TypeError):
      array_qtypes.is_array_qtype(0)  # pytype: disable=wrong-arg-types

  def test_make_array_qtype(self):
    self.assertEqual(
        array_qtypes.make_array_qtype(scalar_qtypes.INT32),
        array_qtypes.ARRAY_INT32,
    )
    self.assertEqual(
        array_qtypes.make_array_qtype(optional_qtypes.OPTIONAL_INT32),
        array_qtypes.ARRAY_INT32,
    )
    with self.assertRaises(TypeError):
      _ = array_qtypes.make_array_qtype(int)  # pytype: disable=wrong-arg-types
    with self.assertRaises(casting.QTypeError):
      _ = (array_qtypes.make_array_qtype(array_qtypes.ARRAY_INT32),)

  def test_array_from_size_and_value_missing_size(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'missing parameter: size'
    ):
      array_qtypes.array_unit(values=[], ids=[])

  @parameterized.parameters(
      # arrays
      (
          array_qtypes.array_boolean,
          optional_qtypes.optional_boolean,
          scalar_utils_test_helpers.BOOLEAN_TEST_DATA,
      ),
      (
          array_qtypes.array_bytes,
          optional_qtypes.optional_bytes,
          scalar_utils_test_helpers.BYTES_TEST_DATA,
      ),
      (
          array_qtypes.array_float32,
          optional_qtypes.optional_float32,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_float64,
          optional_qtypes.optional_float64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_weak_float,
          optional_qtypes.optional_weak_float,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          array_qtypes.array_int32,
          optional_qtypes.optional_int32,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_int64,
          optional_qtypes.optional_int64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_uint64,
          optional_qtypes.optional_uint64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          array_qtypes.array_text,
          optional_qtypes.optional_text,
          scalar_utils_test_helpers.TEXT_TEST_DATA,
      ),
      (
          array_qtypes.array_unit,
          optional_qtypes.optional_unit,
          scalar_utils_test_helpers.UNIT_TEST_DATA,
      ),
      # dense_arrays
      (
          dense_array_qtypes.dense_array_boolean,
          optional_qtypes.optional_boolean,
          scalar_utils_test_helpers.BOOLEAN_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_bytes,
          optional_qtypes.optional_bytes,
          scalar_utils_test_helpers.BYTES_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_float32,
          optional_qtypes.optional_float32,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_float64,
          optional_qtypes.optional_float64,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_weak_float,
          optional_qtypes.optional_weak_float,
          scalar_utils_test_helpers.FLOAT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_int32,
          optional_qtypes.optional_int32,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_int64,
          optional_qtypes.optional_int64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_uint64,
          optional_qtypes.optional_uint64,
          scalar_utils_test_helpers.INDEX_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_text,
          optional_qtypes.optional_text,
          scalar_utils_test_helpers.TEXT_TEST_DATA,
      ),
      (
          dense_array_qtypes.dense_array_unit,
          optional_qtypes.optional_unit,
          scalar_utils_test_helpers.UNIT_TEST_DATA,
      ),
  )
  def test_get_array_item(self, array_factory, item_factory, test_data):
    values, expected_output = zip(*test_data)
    array = array_factory(values)
    for i in range(-len(expected_output), len(expected_output)):
      actual_item = array_qtypes.get_array_item(array, i)
      expected_item = item_factory(expected_output[i])
      self.assertEqual(
          actual_item.fingerprint,
          expected_item.fingerprint,
          f'array[{i}]: {actual_item} != {expected_item}',
      )
    with self.assertRaises(IndexError):
      array_qtypes.get_array_item(array, -len(expected_output) - 1)
    with self.assertRaises(IndexError):
      array_qtypes.get_array_item(array, len(expected_output))

  def test_get_array_item_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 2 required positional arguments: 'array', 'i'"
    ):
      array_qtypes.get_array_item()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError, "missing 1 required positional argument: 'i'"
    ):
      array_qtypes.get_array_item(object())  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected 2 positional arguments, but 3 were given'
    ):
      array_qtypes.get_array_item(object(), object(), object())  # pytype: disable=wrong-arg-count
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an array, got object'
    ):
      array_qtypes.get_array_item(object(), 0)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an array, got arolla.abc.qtype.QType'
    ):
      array_qtypes.get_array_item(arolla_abc.QTYPE, 0)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as an integer"
    ):
      array_qtypes.get_array_item(
          array_qtypes.array_unit([]), object()  # pytype: disable=wrong-arg-types
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'NoneType' object cannot be interpreted as an integer"
    ):
      array_qtypes.get_array_item(
          array_qtypes.array_unit([]), None  # pytype: disable=wrong-arg-types
      )

  def test_get_array_item_missing_optional_error(self):
    with self.assertRaises(scalar_utils.MissingOptionalError):
      array_qtypes.get_array_item(
          array_qtypes.array_unit([]),
          optional_qtypes.optional_int32(None),
      )

  def test_get_array_py_value(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an array, got arolla.abc.qtype.QType'
    ):
      array_qtypes.get_array_py_value(arolla_abc.QTYPE)

  @parameterized.parameters(
      (array_qtypes.array_unit, [[]]),
      (dense_array_qtypes.dense_array_unit, [[]]),
      (array_qtypes.array_unit, [[None], [True]]),
      (dense_array_qtypes.dense_array_unit, [[None], [True]]),
      (array_qtypes.array_unit, itertools.repeat([True], 10240)),
      (dense_array_qtypes.dense_array_unit, itertools.repeat([True], 10240)),
      (array_qtypes.array_float32, [[]]),
      (dense_array_qtypes.dense_array_float32, [[]]),
      (array_qtypes.array_float32, [[None], [1.5]]),
      (dense_array_qtypes.dense_array_float32, [[]]),
      (array_qtypes.array_float32, [[None], [1.5]]),
      (dense_array_qtypes.dense_array_float32, [[None], [1.5]]),
      (array_qtypes.array_float32, itertools.repeat([1.5], 10240)),
      (dense_array_qtypes.dense_array_float32, itertools.repeat([1.5], 10240)),
  )
  def test_array_concat(self, array_factories, valueses):
    valueses = tuple(valueses)
    actual_qvalue = array_qtypes.array_concat(*map(array_factories, valueses))
    expected_qvalue = array_factories(itertools.chain(*valueses))
    self.assertEqual(
        actual_qvalue.fingerprint,
        expected_qvalue.fingerprint,
        f'{actual_qvalue} != {expected_qvalue}',
    )

  def test_array_concat_no_input(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected at least one argument, got none'
    ):
      _ = array_qtypes.array_concat()

  def test_array_concat_non_array(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected all arguments to be arrays, got OPTIONAL_UNIT'
    ):
      _ = array_qtypes.array_concat(optional_qtypes.present())

  def test_array_concat_different_array(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        (
            'expected all arguments to have the same array type, got:'
            ' ARRAY_INT32, ARRAY_BOOLEAN'
        ),
    ):
      _ = array_qtypes.array_concat(
          array_qtypes.array_int32([]),
          array_qtypes.array_boolean([]),
          array_qtypes.array_int32([]),
      )


if __name__ == '__main__':
  absltest.main()
