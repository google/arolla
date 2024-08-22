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

"""Tests for arolla.types.qtype.scalar_qtypes."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import casting
from arolla.types.qtype import dense_array_qtypes
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import scalar_utils
from arolla.types.qtype import scalar_utils_test_helpers


class ScalarBoxingTest(parameterized.TestCase):

  @parameterized.parameters(
      (value, expected_output)
      for value, expected_output in scalar_utils_test_helpers.BOOLEAN_TEST_DATA
      if expected_output is not None
  )
  def test_boolean(self, value, expected_output):
    qvalue = scalar_qtypes.boolean(value)
    self.assertEqual(qvalue.qtype, scalar_qtypes.BOOLEAN)
    self.assertEqual(scalar_utils.py_boolean(qvalue), expected_output)

  @parameterized.parameters(
      value
      for value, expected_output in scalar_utils_test_helpers.BOOLEAN_TEST_DATA
      if value is not None and expected_output is None
  )
  def test_boolean_missing_optional_error(self, value):
    with self.assertRaises(scalar_utils.MissingOptionalError):
      scalar_qtypes.boolean(value)

  def test_boolean_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'NoneType' object cannot be interpreted as a boolean"
    ):
      scalar_qtypes.boolean(None)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as a boolean"
    ):
      scalar_qtypes.boolean(object())

  @parameterized.parameters(
      (value, expected_output)
      for value, expected_output in scalar_utils_test_helpers.BYTES_TEST_DATA
      if expected_output is not None
  )
  def test_bytes(self, value, expected_output):
    qvalue = scalar_qtypes.bytes_(value)
    self.assertEqual(qvalue.qtype, scalar_qtypes.BYTES)
    self.assertEqual(scalar_utils.py_bytes(qvalue), expected_output)

  @parameterized.parameters(
      value
      for value, expected_output in scalar_utils_test_helpers.BYTES_TEST_DATA
      if value is not None and expected_output is None
  )
  def test_bytes_missing_optional_error(self, value):
    with self.assertRaises(scalar_utils.MissingOptionalError):
      scalar_qtypes.bytes_(value)

  def test_bytes_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'NoneType' object cannot be interpreted as bytes"
    ):
      scalar_qtypes.bytes_(None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as bytes"
    ):
      scalar_qtypes.bytes_(object())  # pytype: disable=wrong-arg-types

  @parameterized.parameters(
      (value, expected_output)
      for value, expected_output in scalar_utils_test_helpers.FLOAT_TEST_DATA
      if expected_output is not None
  )
  def test_float(self, value, expected_output):
    with self.subTest('float32'):
      qvalue = scalar_qtypes.float32(value)
      self.assertEqual(qvalue.qtype, scalar_qtypes.FLOAT32)
      self.assertEqual(scalar_utils.py_float(qvalue), expected_output)
    with self.subTest('float64'):
      qvalue = scalar_qtypes.float64(value)
      self.assertEqual(qvalue.qtype, scalar_qtypes.FLOAT64)
      self.assertEqual(scalar_utils.py_float(qvalue), expected_output)
    with self.subTest('weak_float'):
      qvalue = scalar_qtypes.weak_float(value)
      self.assertEqual(qvalue.qtype, scalar_qtypes.WEAK_FLOAT)
      self.assertEqual(scalar_utils.py_float(qvalue), expected_output)

  @parameterized.parameters(
      value
      for value, expected_output in scalar_utils_test_helpers.FLOAT_TEST_DATA
      if value is not None and expected_output is None
  )
  def test_float_missing_optional_error(self, value):
    with self.subTest('float32'):
      with self.assertRaises(scalar_utils.MissingOptionalError):
        scalar_qtypes.float32(value)
    with self.subTest('float64'):
      with self.assertRaises(scalar_utils.MissingOptionalError):
        scalar_qtypes.float64(value)
    with self.subTest('weak_float'):
      with self.assertRaises(scalar_utils.MissingOptionalError):
        scalar_qtypes.weak_float(value)

  def test_float32_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not NoneType'
    ):
      scalar_qtypes.float32(None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not object'
    ):
      scalar_qtypes.float32(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not str'
    ):
      scalar_qtypes.float32('1')  # pytype: disable=wrong-arg-types

  def test_float64_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not NoneType'
    ):
      scalar_qtypes.float64(None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not object'
    ):
      scalar_qtypes.float64(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not str'
    ):
      scalar_qtypes.float64('1')  # pytype: disable=wrong-arg-types

  def test_weak_float_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not NoneType'
    ):
      scalar_qtypes.weak_float(None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not object'
    ):
      scalar_qtypes.weak_float(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not str'
    ):
      scalar_qtypes.weak_float('1')  # pytype: disable=wrong-arg-types

  @parameterized.parameters(
      (value, expected_output)
      for value, expected_output in scalar_utils_test_helpers.INDEX_TEST_DATA
      if expected_output is not None
  )
  def test_int(self, value, expected_output):
    with self.subTest('int32'):
      qvalue = scalar_qtypes.int32(value)
      self.assertEqual(qvalue.qtype, scalar_qtypes.INT32)
      self.assertEqual(scalar_utils.py_index(qvalue), expected_output)
    with self.subTest('int64'):
      qvalue = scalar_qtypes.int64(value)
      self.assertEqual(qvalue.qtype, scalar_qtypes.INT64)
      self.assertEqual(scalar_utils.py_index(qvalue), expected_output)
    with self.subTest('uint64'):
      qvalue = scalar_qtypes.uint64(value)
      self.assertEqual(qvalue.qtype, scalar_qtypes.UINT64)
      self.assertEqual(scalar_utils.py_index(qvalue), expected_output)

  @parameterized.parameters(
      value
      for value, expected_output in scalar_utils_test_helpers.INDEX_TEST_DATA
      if value is not None and expected_output is None
  )
  def test_int_missing_optional_error(self, value):
    with self.subTest('int32'):
      with self.assertRaises(scalar_utils.MissingOptionalError):
        scalar_qtypes.int32(value)
    with self.subTest('int64'):
      with self.assertRaises(scalar_utils.MissingOptionalError):
        scalar_qtypes.int64(value)
    with self.subTest('uint64'):
      with self.assertRaises(scalar_utils.MissingOptionalError):
        scalar_qtypes.uint64(value)

  def test_int32_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'NoneType' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.int32(None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.int32(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'str' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.int32('1')  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'float' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.int32(1.5)  # pytype: disable=wrong-arg-types

  def test_int64_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'NoneType' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.int64(None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.int64(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'str' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.int64('1')  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'float' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.int64(1.5)  # pytype: disable=wrong-arg-types

  def test_uint64_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'NoneType' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.uint64(None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.uint64(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'str' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.uint64('1')  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'float' object cannot be interpreted as an integer"
    ):
      scalar_qtypes.uint64(1.5)  # pytype: disable=wrong-arg-types

  def test_int32_overflow_error(self):
    with self.assertRaises(OverflowError):
      scalar_qtypes.int32(2**31)
    with self.assertRaises(OverflowError):
      scalar_qtypes.int32(-(2**31) - 1)
    with self.assertRaises(OverflowError):
      scalar_qtypes.int32(2**1024)
    with self.assertRaises(OverflowError):
      scalar_qtypes.int32(-(2**1024))

  def test_int64_overflow_error(self):
    with self.assertRaises(OverflowError):
      scalar_qtypes.int64(2**63)
    with self.assertRaises(OverflowError):
      scalar_qtypes.int64(-(2**63) - 1)
    with self.assertRaises(OverflowError):
      scalar_qtypes.int64(2**1024)
    with self.assertRaises(OverflowError):
      scalar_qtypes.int64(-(2**1024))

  def test_uint64_overflow_error(self):
    with self.assertRaises(OverflowError):
      scalar_qtypes.uint64(2**64)
    with self.assertRaises(OverflowError):
      scalar_qtypes.uint64(-1)
    with self.assertRaises(OverflowError):
      scalar_qtypes.uint64(2**1024)

  @parameterized.parameters(
      (value, expected_output)
      for value, expected_output in scalar_utils_test_helpers.TEXT_TEST_DATA
      if expected_output is not None
  )
  def test_text(self, value, expected_output):
    qvalue = scalar_qtypes.text(value)
    self.assertEqual(qvalue.qtype, scalar_qtypes.TEXT)
    self.assertEqual(scalar_utils.py_text(qvalue), expected_output)

  @parameterized.parameters(
      value
      for value, expected_output in scalar_utils_test_helpers.TEXT_TEST_DATA
      if value is not None and expected_output is None
  )
  def test_text_missing_optional_error(self, value):
    with self.assertRaises(scalar_utils.MissingOptionalError):
      scalar_qtypes.text(value)

  def test_text_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'NoneType' object cannot be interpreted as a text"
    ):
      scalar_qtypes.text(None)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as a text"
    ):
      scalar_qtypes.text(object())

  def test_unit(self):
    qvalue = scalar_qtypes.unit()
    self.assertEqual(qvalue.qtype, scalar_qtypes.UNIT)
    self.assertEqual(scalar_utils.py_unit(qvalue), True)

  def test_py_c_api_casting_regression(self):
    self.assertEqual(scalar_utils.py_index(scalar_qtypes.int32(-1)), -1)
    self.assertEqual(scalar_utils.py_index(scalar_qtypes.int64(-1)), -1)
    self.assertEqual(
        scalar_utils.py_index(scalar_qtypes.uint64(2**64 - 1)), 2**64 - 1
    )
    self.assertEqual(scalar_utils.py_float(scalar_qtypes.float32(-1)), -1.0)
    self.assertEqual(scalar_utils.py_float(scalar_qtypes.float64(-1)), -1.0)
    self.assertEqual(scalar_utils.py_float(scalar_qtypes.weak_float(-1)), -1.0)

  @parameterized.parameters(
      (scalar_qtypes.UNIT, True),
      (scalar_qtypes.BOOLEAN, True),
      (scalar_qtypes.BYTES, True),
      (scalar_qtypes.TEXT, True),
      (scalar_qtypes.INT32, True),
      (scalar_qtypes.INT64, True),
      (scalar_qtypes.FLOAT32, True),
      (scalar_qtypes.FLOAT64, True),
      (scalar_qtypes.WEAK_FLOAT, True),
      (optional_qtypes.OPTIONAL_INT64, False),
      (array_qtypes.ARRAY_INT32, False),
      (dense_array_qtypes.DENSE_ARRAY_INT32, False),
  )
  def test_is_scalar_qtype(self, qtype, expected_output):
    self.assertEqual(scalar_qtypes.is_scalar_qtype(qtype), expected_output)

  def test_is_scalar_qtype_type_error(self):
    with self.assertRaisesRegex(TypeError, 'expected QType, got qtype: int'):
      _ = scalar_qtypes.is_scalar_qtype(5)  # pytype: disable=wrong-arg-types

  @parameterized.parameters(
      (scalar_qtypes.INT32, scalar_qtypes.INT32),
      (optional_qtypes.OPTIONAL_INT32, scalar_qtypes.INT32),
      (array_qtypes.ARRAY_INT32, scalar_qtypes.INT32),
      (dense_array_qtypes.DENSE_ARRAY_INT32, scalar_qtypes.INT32),
      (optional_qtypes.OPTIONAL_WEAK_FLOAT, scalar_qtypes.WEAK_FLOAT),
  )
  def test_get_scalar_qtype(self, qtype, expected_scalar_qtype):
    self.assertEqual(
        scalar_qtypes.get_scalar_qtype(qtype), expected_scalar_qtype
    )

  def test_get_scalar_qtype_qtype_error(self):
    with self.assertRaisesRegex(
        casting.QTypeError, 'no scalar type for: QTYPE'
    ):
      _ = scalar_qtypes.get_scalar_qtype(arolla_abc.QTYPE)

  def test_get_scalar_qtype_type_error(self):
    with self.assertRaisesRegex(TypeError, 'expected QType, got qtype: object'):
      _ = scalar_qtypes.get_scalar_qtype(object())  # pytype: disable=wrong-arg-types


if __name__ == '__main__':
  absltest.main()
