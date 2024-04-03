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

"""Tests for QValue class."""

import gc
import weakref

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import dummy_types
from arolla.abc import qtype as abc_qtype

DUMMY_VALUE = dummy_types.make_dummy_value().qtype


class QValueSpecializationTest(parameterized.TestCase):

  def tearDown(self):
    abc_qtype.remove_qvalue_specialization(DUMMY_VALUE)
    super().tearDown()

  def test_repr(self):
    class DummyQValue(abc_qtype.QValue):
      pass

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    self.assertEqual(repr(dummy_types.make_dummy_value()), 'dummy-value')

  def test_no_bool(self):
    class DummyQValue(abc_qtype.QValue):
      pass

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        (
            '__bool__ disabled for DummyQValue'
        ),
    ):
      _ = bool(dummy_types.make_dummy_value())

  def test_bool(self):
    class DummyQValue(abc_qtype.QValue):

      def __bool__(self):
        return False

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    self.assertFalse(dummy_types.make_dummy_value())

  def test_hash(self):
    class DummyQValue(abc_qtype.QValue):
      pass

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "unhashable type: 'DummyQValue'"
    ):
      _ = hash(dummy_types.make_dummy_value())

  def test_arolla_init(self):
    check = False

    class DummyQValue(abc_qtype.QValue):

      def _arolla_init_(self):
        super()._arolla_init_()
        nonlocal check
        check = True

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    _ = dummy_types.make_dummy_value()
    self.assertTrue(check)

  def test_arolla_init_raises_exception(self):
    class DummyQValue(abc_qtype.QValue):

      def _arolla_init_(self):
        raise NotImplementedError

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    with self.assertRaises(NotImplementedError):
      _ = dummy_types.make_dummy_value()

  def test_arolla_init_is_non_regular_method(self):
    class DummyQValue(abc_qtype.QValue):
      _arolla_init_ = None

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    with self.assertRaisesWithLiteralMatch(
        TypeError, "'NoneType' object is not callable"
    ):
      _ = dummy_types.make_dummy_value()

  def test_no_richcompare(self):
    class DummyQValue(abc_qtype.QValue):
      pass

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    x = dummy_types.make_dummy_value()
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        (
            '__eq__ and __ne__ disabled for DummyQValue'
        ),
    ):
      _ = x == abc_qtype.QTYPE
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        (
            '__eq__ and __ne__ disabled for DummyQValue'
        ),
    ):
      _ = x != abc_qtype.QTYPE
    with self.assertRaises(TypeError):
      _ = x < abc_qtype.QTYPE
    with self.assertRaises(TypeError):
      _ = x <= abc_qtype.QTYPE
    with self.assertRaises(TypeError):
      _ = x > abc_qtype.QTYPE
    with self.assertRaises(TypeError):
      _ = x >= abc_qtype.QTYPE

  def test_fingerprint(self):
    class DummyQValue(abc_qtype.QValue):
      pass

    abc_qtype.register_qvalue_specialization(DUMMY_VALUE, DummyQValue)
    x = dummy_types.make_dummy_value()
    y = dummy_types.make_dummy_value()
    self.assertIsInstance(x.fingerprint, abc_qtype.Fingerprint)
    self.assertEqual(x.fingerprint, y.fingerprint)

  def test_fingerprint_hash(self):
    x = dummy_types.make_dummy_value()
    self.assertIsInstance(x._fingerprint_hash, int)

  def test_weakref(self):
    x = dummy_types.make_dummy_value()
    weak_x = weakref.ref(x)
    self.assertIs(weak_x(), x)
    x = None
    gc.collect()
    self.assertIsNone(weak_x())

  def test_qvalue_specialization_key(self):
    qvalue = dummy_types.make_dummy_value()
    self.assertEqual(
        qvalue._specialization_key, '::arolla::testing::DummyValue'
    )
    self.assertEqual(
        qvalue.qtype._specialization_key, '::arolla::testing::DummyValueQType'
    )


if __name__ == '__main__':
  absltest.main()
