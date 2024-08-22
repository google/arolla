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

"""Tests for arolla.types.qtype.optional_qtypes."""

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


class OptionalQTypeTest(parameterized.TestCase):

  @parameterized.parameters(*scalar_utils_test_helpers.BOOLEAN_TEST_DATA)
  def test_optional_boolean(self, value, expected_output):
    qvalue = optional_qtypes.optional_boolean(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_BOOLEAN)
    self.assertEqual(scalar_utils.py_boolean(qvalue), expected_output)

  def test_optional_boolean_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as a boolean"
    ):
      optional_qtypes.optional_boolean(object())

  @parameterized.parameters(*scalar_utils_test_helpers.BYTES_TEST_DATA)
  def test_optional_bytes(self, value, expected_output):
    qvalue = optional_qtypes.optional_bytes(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_BYTES)
    self.assertEqual(scalar_utils.py_bytes(qvalue), expected_output)

  def test_optional_bytes_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as bytes"
    ):
      optional_qtypes.optional_bytes(object())  # pytype: disable=wrong-arg-types

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_optional_float32(self, value, expected_output):
    qvalue = optional_qtypes.optional_float32(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_FLOAT32)
    self.assertEqual(scalar_utils.py_float(qvalue), expected_output)

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_optional_float64(self, value, expected_output):
    qvalue = optional_qtypes.optional_float64(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_FLOAT64)
    self.assertEqual(scalar_utils.py_float(qvalue), expected_output)

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_optional_weak_float(self, value, expected_output):
    qvalue = optional_qtypes.optional_weak_float(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_WEAK_FLOAT)
    self.assertEqual(scalar_utils.py_float(qvalue), expected_output)

  def test_optional_float32_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not object'
    ):
      optional_qtypes.optional_float32(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not str'
    ):
      optional_qtypes.optional_float32('1')  # pytype: disable=wrong-arg-types

  def test_optional_float64_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not object'
    ):
      optional_qtypes.optional_float64(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not str'
    ):
      optional_qtypes.optional_float64('1')  # pytype: disable=wrong-arg-types

  def test_optional_weak_float_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not object'
    ):
      optional_qtypes.optional_weak_float(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'must be real number, not str'
    ):
      optional_qtypes.optional_weak_float('1')  # pytype: disable=wrong-arg-types

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_optional_int32(self, value, expected_output):
    qvalue = optional_qtypes.optional_int32(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_INT32)
    self.assertEqual(scalar_utils.py_index(qvalue), expected_output)

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_optional_int64(self, value, expected_output):
    qvalue = optional_qtypes.optional_int64(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_INT64)
    self.assertEqual(scalar_utils.py_index(qvalue), expected_output)

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_optional_uint64(self, value, expected_output):
    qvalue = optional_qtypes.optional_uint64(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_UINT64)
    self.assertEqual(scalar_utils.py_index(qvalue), expected_output)

  def test_optional_int32_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_int32(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'str' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_int32('1')  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'float' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_int32(1.5)  # pytype: disable=wrong-arg-types

  def test_optional_int64_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_int64(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'str' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_int64('1')  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'float' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_int64(1.5)  # pytype: disable=wrong-arg-types

  def test_optional_uint64_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_uint64(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'str' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_uint64('1')  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'float' object cannot be interpreted as an integer"
    ):
      optional_qtypes.optional_uint64(1.5)  # pytype: disable=wrong-arg-types

  def test_optional_int32_overflow_error(self):
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_int32(2**31)
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_int32(-(2**31) - 1)
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_int32(2**1024)
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_int32(-(2**1024))

  def test_optional_int64_overflow_error(self):
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_int64(2**63)
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_int64(-(2**63) - 1)
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_int64(2**1024)
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_int64(-(2**1024))

  def test_optional_uint64_overflow_error(self):
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_uint64(2**64)
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_uint64(-1)
    with self.assertRaises(OverflowError):
      optional_qtypes.optional_uint64(2**1024)

  @parameterized.parameters(*scalar_utils_test_helpers.TEXT_TEST_DATA)
  def test_optional_text(self, value, expected_output):
    qvalue = optional_qtypes.optional_text(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_TEXT)
    self.assertEqual(scalar_utils.py_text(qvalue), expected_output)

  def test_optional_text_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as a text"
    ):
      optional_qtypes.optional_text(object())

  @parameterized.parameters(*scalar_utils_test_helpers.UNIT_TEST_DATA)
  def test_optional_unit(self, value, expected_output):
    qvalue = optional_qtypes.optional_unit(value)
    self.assertEqual(qvalue.qtype, optional_qtypes.OPTIONAL_UNIT)
    self.assertEqual(scalar_utils.py_unit(qvalue), expected_output)

  def test_optional_unit_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as a unit"
    ):
      optional_qtypes.optional_unit(object())

  @parameterized.parameters(
      (optional_qtypes.OPTIONAL_UNIT, True),
      (optional_qtypes.OPTIONAL_BOOLEAN, True),
      (optional_qtypes.OPTIONAL_BYTES, True),
      (optional_qtypes.OPTIONAL_TEXT, True),
      (optional_qtypes.OPTIONAL_INT32, True),
      (optional_qtypes.OPTIONAL_INT64, True),
      (optional_qtypes.OPTIONAL_FLOAT32, True),
      (optional_qtypes.OPTIONAL_FLOAT64, True),
      (optional_qtypes.OPTIONAL_WEAK_FLOAT, True),
      (scalar_qtypes.INT64, False),
      (array_qtypes.ARRAY_INT32, False),
      (dense_array_qtypes.DENSE_ARRAY_INT32, False),
  )
  def test_is_optional_qtype(self, qtype, expected_output):
    self.assertEqual(optional_qtypes.is_optional_qtype(qtype), expected_output)

  def test_is_optional_qtype_type_error(self):
    with self.assertRaises(TypeError):
      optional_qtypes.is_optional_qtype(0)  # pytype: disable=wrong-arg-types

  @parameterized.parameters(
      (scalar_qtypes.UNIT, optional_qtypes.OPTIONAL_UNIT),
      (scalar_qtypes.BOOLEAN, optional_qtypes.OPTIONAL_BOOLEAN),
      (scalar_qtypes.BYTES, optional_qtypes.OPTIONAL_BYTES),
      (scalar_qtypes.TEXT, optional_qtypes.OPTIONAL_TEXT),
      (scalar_qtypes.INT32, optional_qtypes.OPTIONAL_INT32),
      (scalar_qtypes.INT64, optional_qtypes.OPTIONAL_INT64),
      (scalar_qtypes.FLOAT32, optional_qtypes.OPTIONAL_FLOAT32),
      (scalar_qtypes.FLOAT64, optional_qtypes.OPTIONAL_FLOAT64),
      (scalar_qtypes.WEAK_FLOAT, optional_qtypes.OPTIONAL_WEAK_FLOAT),
  )
  def test_make_optional_qtype(self, value_qtype, expected_qtype):
    self.assertEqual(
        optional_qtypes.make_optional_qtype(value_qtype), expected_qtype
    )
    self.assertEqual(
        optional_qtypes.make_optional_qtype(expected_qtype), expected_qtype
    )

  def test_make_optional_qtype_type_error(self):
    with self.assertRaisesRegex(TypeError, 'expected QType, got qtype: int'):
      _ = optional_qtypes.is_optional_qtype(5)  # pytype: disable=wrong-arg-types

  def test_make_optional_qtype_qtype_error(self):
    with self.assertRaisesRegex(
        casting.QTypeError, 'expected a scalar qtype, got qtype=QTYPE'
    ):
      _ = optional_qtypes.make_optional_qtype(arolla_abc.QTYPE)


if __name__ == '__main__':
  absltest.main()
