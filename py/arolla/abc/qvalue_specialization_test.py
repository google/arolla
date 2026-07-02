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

"""Tests for qvalue specialization."""

import gc
import re
import sys

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import dummy_types
from arolla.abc import qtype as abc_qtype


class DummyQValue1(abc_qtype.QValue):
  pass


class DummyQValue2(abc_qtype.QValue):
  pass


class DummyQValue3(abc_qtype.QValue):
  pass


DUMMY_VALUE = dummy_types.make_dummy_value().qtype


class QValueSpecializationTest(parameterized.TestCase):

  def tearDown(self):
    abc_qtype.remove_qvalue_specialization(DUMMY_VALUE)
    abc_qtype.remove_qvalue_specialization('::arolla::testing::DummyValue')
    super().tearDown()

  def test_specialization_by_qtype(self):
    self.assertNotIsInstance(dummy_types.make_dummy_value(), DummyQValue1)
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue1)
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue1)

  def test_override_specialization_by_qtype(self):
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue1)
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue2)
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue2)

  def test_remove_specialization_by_qtype(self):
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue1)
    abc_qtype.remove_qvalue_specialization(DUMMY_VALUE)
    self.assertNotIsInstance(dummy_types.make_dummy_value(), DummyQValue1)

  def test_specialization_by_key(self):
    self.assertNotIsInstance(dummy_types.make_dummy_value(), DummyQValue1)
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue1
    )
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue1)

  def test_override_specialization_by_key(self):
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue1
    )
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue2
    )
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue2)

  def test_remove_specialization_by_key(self):
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue1
    )
    abc_qtype.remove_qvalue_specialization('::arolla::testing::DummyValue')
    self.assertNotIsInstance(dummy_types.make_dummy_value(), DummyQValue1)

  def test_specialisation_priority(self):
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValueQType', DummyQValue1
    )
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue2)
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue3
    )
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue3)
    abc_qtype.remove_qvalue_specialization('::arolla::testing::DummyValue')
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue2)
    abc_qtype.remove_qvalue_specialization(DUMMY_VALUE)
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue1)
    abc_qtype.remove_qvalue_specialization('::arolla::testing::DummyValueQType')
    self.assertIs(type(dummy_types.make_dummy_value()), abc_qtype.QValue)

  def test_error_not_a_type(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected subclass of QValue, got None'
    ):
      abc_qtype.register_qvalue_specialization(DUMMY_VALUE, None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected subclass of QValue, got None'
    ):
      abc_qtype.register_qvalue_specialization(
          '::arolla::testing::DummyValue', None  # pyrefly: ignore[bad-argument-type]
      )  # pytype: disable=wrong-arg-types

  def test_error_not_qvalue_subtype(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected subclass of QValue, got int'
    ):
      abc_qtype.register_qvalue_specialization(DUMMY_VALUE, int)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected subclass of QValue, got int'
    ):
      abc_qtype.register_qvalue_specialization(
          '::arolla::testing::DummyValue', int  # pyrefly: ignore[bad-argument-type]
      )  # pytype: disable=wrong-arg-types

  def test_error_empty_key(self):
    with self.assertRaisesRegex(ValueError, re.escape('key is empty')):
      abc_qtype.register_qvalue_specialization('', DummyQValue1)

  def test_error_key_none(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('expected str or QType, got None')
    ):
      abc_qtype.register_qvalue_specialization(None, DummyQValue1)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        TypeError, re.escape('expected str or QType, got None')
    ):
      abc_qtype.remove_qvalue_specialization(None)  # pytype: disable=wrong-arg-types

  def test_error_qtype_qtype(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('QValue specialization for QTYPE cannot be changed'),
    ):
      abc_qtype.register_qvalue_specialization(abc_qtype.QTYPE, DummyQValue1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('QValue specialization for QTYPE cannot be changed'),
    ):
      abc_qtype.remove_qvalue_specialization(abc_qtype.QTYPE)

  def test_specialization_by_qtype_refcount(self):
    class CustomQValue(abc_qtype.QValue):
      pass

    gc.collect()
    base_refcount = sys.getrefcount(CustomQValue)
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, CustomQValue)
    gc.collect()
    self.assertEqual(sys.getrefcount(CustomQValue), base_refcount + 1)
    abc_qtype.remove_qvalue_specialization(DUMMY_VALUE)
    gc.collect()
    self.assertEqual(sys.getrefcount(CustomQValue), base_refcount)

  def test_specialization_by_key_refcount(self):
    class CustomQValue(abc_qtype.QValue):
      pass

    gc.collect()
    base_refcount = sys.getrefcount(CustomQValue)
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', CustomQValue
    )
    gc.collect()
    self.assertEqual(sys.getrefcount(CustomQValue), base_refcount + 1)
    abc_qtype.remove_qvalue_specialization('::arolla::testing::DummyValue')
    gc.collect()
    self.assertEqual(sys.getrefcount(CustomQValue), base_refcount)


if __name__ == '__main__':
  absltest.main()
