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

"""Tests for arolla.types.qtype.scalar_utils."""

from absl.testing import absltest
from absl.testing import parameterized
import numpy
from arolla.abc import abc as arolla_abc
from arolla.types.qtype import scalar_utils
from arolla.types.qtype import scalar_utils_test_helpers


class ScalarUtilsTest(parameterized.TestCase):

  @parameterized.parameters(*scalar_utils_test_helpers.BOOLEAN_TEST_DATA)
  def test_py_boolean(self, value, expected_output):
    self.assertEqual(scalar_utils.py_boolean(value), expected_output)

  def test_py_boolean_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as a boolean"
    ):
      scalar_utils.py_boolean(object())
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "'arolla.abc.qtype.QType' object cannot be interpreted as a boolean",
    ):
      scalar_utils.py_boolean(arolla_abc.QTYPE)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'numpy.ndarray' object cannot be interpreted as a boolean"
    ):
      scalar_utils.py_boolean(numpy.array([True], numpy.bool_))
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'numpy.int32' object cannot be interpreted as a boolean"
    ):
      scalar_utils.py_boolean(numpy.int32(1))

  @parameterized.parameters(*scalar_utils_test_helpers.BYTES_TEST_DATA)
  def test_py_bytes(self, value, expected_output):
    self.assertEqual(scalar_utils.py_bytes(value), expected_output)

  def test_py_bytes_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as bytes"
    ):
      scalar_utils.py_bytes(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'str' object cannot be interpreted as bytes"
    ):
      scalar_utils.py_bytes("123")  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "'arolla.abc.qtype.QType' object cannot be interpreted as bytes",
    ):
      scalar_utils.py_bytes(arolla_abc.QTYPE)  # pytype: disable=wrong-arg-types

  @parameterized.parameters(*scalar_utils_test_helpers.FLOAT_TEST_DATA)
  def test_py_float(self, value, expected_output):
    self.assertEqual(scalar_utils.py_float(value), expected_output)

  def test_py_float_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "must be real number, not object"
    ):
      scalar_utils.py_float(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "must be real number, not str"
    ):
      scalar_utils.py_float("123")  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "must be real number, not arolla.abc.qtype.QType"
    ):
      scalar_utils.py_float(arolla_abc.QTYPE)  # pytype: disable=wrong-arg-types

  @parameterized.parameters(*scalar_utils_test_helpers.INDEX_TEST_DATA)
  def test_py_index(self, value, expected_output):
    self.assertEqual(scalar_utils.py_index(value), expected_output)

  def test_py_index_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as an integer"
    ):
      scalar_utils.py_index(object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'str' object cannot be interpreted as an integer"
    ):
      scalar_utils.py_index("123")  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "'arolla.abc.qtype.QType' object cannot be interpreted as an integer",
    ):
      scalar_utils.py_index(arolla_abc.QTYPE)  # pytype: disable=wrong-arg-types

  @parameterized.parameters(*scalar_utils_test_helpers.TEXT_TEST_DATA)
  def test_py_text(self, value, expected_output):
    self.assertEqual(scalar_utils.py_text(value), expected_output)

  def test_py_text_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as a text"
    ):
      scalar_utils.py_text(object())
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "'arolla.abc.qtype.QType' object cannot be interpreted as a text",
    ):
      scalar_utils.py_text(arolla_abc.QTYPE)

  @parameterized.parameters(*scalar_utils_test_helpers.UNIT_TEST_DATA)
  def test_py_unit(self, value, expected_output):
    self.assertEqual(scalar_utils.py_unit(value), expected_output)

  def test_py_unit_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object cannot be interpreted as a unit"
    ):
      scalar_utils.py_unit(object())
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "'arolla.abc.qtype.QType' object cannot be interpreted as a unit",
    ):
      scalar_utils.py_unit(arolla_abc.QTYPE)

  def test_py_unit_value_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, "False cannot be interpreted as a unit"
    ):
      scalar_utils.py_unit(False)


if __name__ == "__main__":
  absltest.main()
