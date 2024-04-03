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

"""Tests for arolla.abc.qtype."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import dummy_types
from arolla.abc import qtype as abc_qtype

DUMMY_VALUE = dummy_types.make_dummy_value().qtype
DUMMY_CONTAINER = dummy_types.make_dummy_container().qtype


class QTypeTest(parameterized.TestCase):

  def tearDown(self):
    abc_qtype.remove_qvalue_specialization(DUMMY_VALUE)
    super().tearDown()

  def test_type(self):
    self.assertEqual(abc_qtype.QValue.__name__, 'QValue')
    self.assertEqual(abc_qtype.QType.__name__, 'QType')
    self.assertNotEqual(abc_qtype.QType, abc_qtype.QValue)
    self.assertTrue(issubclass(abc_qtype.QType, abc_qtype.QValue))
    self.assertIsInstance(abc_qtype.QTYPE, abc_qtype.QType)
    self.assertIsInstance(abc_qtype.NOTHING, abc_qtype.QType)
    self.assertIsInstance(abc_qtype.UNSPECIFIED, abc_qtype.QType)
    self.assertEqual(repr(abc_qtype.QTYPE), 'QTYPE')
    self.assertEqual(repr(abc_qtype.NOTHING), 'NOTHING')
    self.assertEqual(repr(abc_qtype.UNSPECIFIED), 'UNSPECIFIED')
    self.assertEqual(repr(abc_qtype.unspecified()), 'unspecified')
    self.assertEqual(abc_qtype.unspecified().qtype, abc_qtype.UNSPECIFIED)
    self.assertIsInstance(DUMMY_VALUE, abc_qtype.QType)

  def test_non_qtype_value(self):
    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, abc_qtype.QType)
    with self.assertRaisesRegex(
        TypeError, re.escape('expected QTYPE, got DUMMY_VALUE')
    ):
      _ = dummy_types.make_dummy_value()

  def test_name(self):
    self.assertEqual(abc_qtype.QTYPE.name, 'QTYPE')
    self.assertEqual(DUMMY_VALUE.name, 'DUMMY_VALUE')

  def test_bool(self):
    self.assertTrue(bool(abc_qtype.QTYPE))
    self.assertTrue(bool(DUMMY_VALUE))

  def test_equal(self):
    qtypes = [
        DUMMY_VALUE,
        DUMMY_CONTAINER,
        None,
    ]
    for i, lhs in enumerate(qtypes):
      for j, rhs in enumerate(qtypes):
        if i == j:
          self.assertEqual(lhs, rhs)
        else:
          self.assertNotEqual(lhs, rhs)

  def test_equal_commutative_error(self):
    class DummyQValue(abc_qtype.QValue):

      def __eq__(self, other):
        raise NotImplementedError

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)

    dummy_value = dummy_types.make_dummy_value()
    with self.assertRaises(NotImplementedError):
      _ = dummy_value == abc_qtype.QTYPE
    with self.assertRaises(NotImplementedError):
      _ = abc_qtype.QTYPE == dummy_value

  def test_hash(self):
    qtypes = [
        abc_qtype.QTYPE,
        abc_qtype.NOTHING,
        DUMMY_VALUE,
        DUMMY_CONTAINER,
        None,
    ]
    for lhs in enumerate(qtypes):
      for rhs in enumerate(qtypes):
        if lhs == rhs:
          self.assertEqual(hash(lhs), hash(rhs))
        else:
          self.assertNotEqual(hash(lhs), hash(rhs))

  def test_value_qtype(self):
    self.assertIsNone(DUMMY_VALUE.value_qtype)
    self.assertEqual(DUMMY_CONTAINER.value_qtype, DUMMY_VALUE)

  def test_get_field_qtypes_error(self):
    with self.assertRaises(TypeError):
      _ = abc_qtype.get_field_qtypes(None)  # pytype: disable=wrong-arg-types
    with self.assertRaises(TypeError):
      _ = abc_qtype.get_field_qtypes('qtype')  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a qtype with fields, got DUMMY_CONTAINER'),
    ):
      _ = abc_qtype.get_field_qtypes(DUMMY_CONTAINER)

  def test_unspecified(self):
    unspecified = abc_qtype.Unspecified()
    self.assertIsInstance(unspecified, abc_qtype.Unspecified)
    self.assertEqual(unspecified.qtype, abc_qtype.UNSPECIFIED)
    self.assertTrue(bool(unspecified == unspecified))  # pylint: disable=comparison-with-itself
    self.assertFalse(bool(unspecified != unspecified))  # pylint: disable=comparison-with-itself
    self.assertFalse(bool(unspecified == abc_qtype.QTYPE))
    self.assertTrue(bool(unspecified != abc_qtype.QTYPE))
    self.assertIsNone(unspecified.py_value())


if __name__ == '__main__':
  absltest.main()
