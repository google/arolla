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

"""Tests for qvalue specialization."""

import re

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

  def testSpecializationByQType(self):
    self.assertNotIsInstance(dummy_types.make_dummy_value(), DummyQValue1)
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue1)
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue1)

  def testOverrideSpecializationByQType(self):
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue1)
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue2)
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue2)

  def testRemoveSpecializationByQType(self):
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue1)
    abc_qtype.remove_qvalue_specialization(DUMMY_VALUE)
    self.assertNotIsInstance(dummy_types.make_dummy_value(), DummyQValue1)

  def testSpecializationByKey(self):
    self.assertNotIsInstance(dummy_types.make_dummy_value(), DummyQValue1)
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue1
    )
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue1)

  def testOverrideSpecializationByKey(self):
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue1
    )
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue2
    )
    self.assertIsInstance(dummy_types.make_dummy_value(), DummyQValue2)

  def testRemoveSpecializationByKey(self):
    abc_qtype.register_qvalue_specialization(
        '::arolla::testing::DummyValue', DummyQValue1
    )
    abc_qtype.remove_qvalue_specialization('::arolla::testing::DummyValue')
    self.assertNotIsInstance(dummy_types.make_dummy_value(), DummyQValue1)

  def testSpecialisationPriority(self):
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

  def testErrorNotAType(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected subclass of QValue, got None'
    ):
      abc_qtype.register_qvalue_specialization(DUMMY_VALUE, None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected subclass of QValue, got None'
    ):
      abc_qtype.register_qvalue_specialization(
          '::arolla::testing::DummyValue', None
      )  # pytype: disable=wrong-arg-types

  def testErrorNotQValueSubtype(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected subclass of QValue, got int'
    ):
      abc_qtype.register_qvalue_specialization(DUMMY_VALUE, int)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected subclass of QValue, got int'
    ):
      abc_qtype.register_qvalue_specialization(
          '::arolla::testing::DummyValue', int
      )  # pytype: disable=wrong-arg-types

  def testErrorEmptyKey(self):
    with self.assertRaisesRegex(ValueError, re.escape('key is empty')):
      abc_qtype.register_qvalue_specialization('', DummyQValue1)

  def testErrorKeyNone(self):
    with self.assertRaisesRegex(
        TypeError, re.escape('expected str or QType, got None')
    ):
      abc_qtype.register_qvalue_specialization(None, DummyQValue1)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        TypeError, re.escape('expected str or QType, got None')
    ):
      abc_qtype.remove_qvalue_specialization(None)  # pytype: disable=wrong-arg-types

  def testErrorQTypeQType(self):
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


if __name__ == '__main__':
  absltest.main()
