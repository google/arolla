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

"""Tests for arolla.types.qvalue.optional_qvalue."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.types.qtype import optional_qtype as rl_optional_qtype
from arolla.types.qtype import scalar_utils as rl_scalar_utils
from arolla.types.qvalue import optional_qvalue as rl_optional_qvalue
from arolla.types.qvalue import qvalue_mixins as rl_qvalue_mixins


class OptionalTest(parameterized.TestCase):

  def testInheritance(self):
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalScalar,
            rl_qvalue_mixins.PresenceQValueMixin,
        )
    )
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalUnit, rl_optional_qvalue.OptionalScalar
        )
    )
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalBoolean,
            rl_optional_qvalue.OptionalScalar,
        )
    )
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalBytes, rl_optional_qvalue.OptionalScalar
        )
    )
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalText, rl_optional_qvalue.OptionalScalar
        )
    )
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalInt, rl_optional_qvalue.OptionalScalar
        )
    )
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalInt,
            rl_qvalue_mixins.IntegralArithmeticQValueMixin,
        )
    )
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalFloat, rl_optional_qvalue.OptionalScalar
        )
    )
    self.assertTrue(
        issubclass(
            rl_optional_qvalue.OptionalFloat,
            rl_qvalue_mixins.FloatingPointArithmeticQValueMixin,
        )
    )

  def testOptionalUnit_Missing(self):
    x = rl_optional_qtype.missing_unit()
    self.assertIsInstance(x, rl_optional_qvalue.OptionalUnit)
    self.assertFalse(x.is_present)
    self.assertIsNone(x.py_value())
    self.assertFalse(x.__bool__())
    self.assertEqual(hash(x), hash(None))
    self.assertEqual(repr(x), 'missing')

  def testOptionalUnit_Present(self):
    x = rl_optional_qtype.present_unit()
    self.assertIsInstance(x, rl_optional_qvalue.OptionalUnit)
    self.assertTrue(x.is_present)
    self.assertTrue(x.py_value())
    self.assertTrue(x.__bool__())
    self.assertEqual(hash(x), hash(True))
    self.assertEqual(repr(x), 'present')

  def testOptionalBoolean_Missing(self):
    x = rl_optional_qtype.optional_boolean(None)
    self.assertIsInstance(x, rl_optional_qvalue.OptionalBoolean)
    self.assertFalse(x.is_present)
    self.assertIsNone(x.py_value())
    with self.assertRaises(rl_scalar_utils.MissingOptionalError):
      x.__bool__()
    self.assertEqual(hash(x), hash(None))
    self.assertIsNone(rl_scalar_utils.py_boolean(x))

  def testOptionalBoolean_False(self):
    x = rl_optional_qtype.optional_boolean(False)
    self.assertIsInstance(x, rl_optional_qvalue.OptionalBoolean)
    self.assertTrue(x.is_present)
    self.assertFalse(x.py_value())
    self.assertFalse(x.__bool__())
    self.assertEqual(hash(x), hash(False))
    self.assertFalse(rl_scalar_utils.py_boolean(x))

  def testOptionalBoolean_True(self):
    x = rl_optional_qtype.optional_boolean(True)
    self.assertIsInstance(x, rl_optional_qvalue.OptionalBoolean)
    self.assertTrue(x.is_present)
    self.assertTrue(x.py_value())
    self.assertTrue(x.__bool__())
    self.assertEqual(hash(x), hash(True))
    self.assertTrue(rl_scalar_utils.py_boolean(x))

  def testOptionalBytes_Missing(self):
    x = rl_optional_qtype.optional_bytes(None)
    self.assertIsInstance(x, rl_optional_qvalue.OptionalBytes)
    self.assertFalse(x.is_present)
    self.assertIsNone(x.py_value())
    with self.assertRaises(TypeError):
      x.__bool__()
    with self.assertRaises(rl_scalar_utils.MissingOptionalError):
      x.__bytes__()
    self.assertEqual(hash(x), hash(None))
    self.assertIsNone(rl_scalar_utils.py_bytes(x))

  def testOptionalBytes_Present(self):
    x = rl_optional_qtype.optional_bytes(b'foo')
    self.assertIsInstance(x, rl_optional_qvalue.OptionalBytes)
    self.assertTrue(x.is_present)
    self.assertEqual(x.py_value(), b'foo')
    with self.assertRaises(TypeError):
      x.__bool__()
    self.assertEqual(x.__bytes__(), b'foo')
    self.assertEqual(hash(x), hash(b'foo'))
    self.assertEqual(rl_scalar_utils.py_bytes(x), b'foo')

  def testOptionalText_Missing(self):
    x = rl_optional_qtype.optional_text(None)
    self.assertIsInstance(x, rl_optional_qvalue.OptionalText)
    self.assertFalse(x.is_present)
    self.assertIsNone(x.py_value())
    with self.assertRaises(TypeError):
      x.__bool__()
    self.assertEqual(hash(x), hash(None))
    self.assertIsNone(rl_scalar_utils.py_text(x))

  def testOptionalText_Present(self):
    x = rl_optional_qtype.optional_text('bar')
    self.assertIsInstance(x, rl_optional_qvalue.OptionalText)
    self.assertTrue(x.is_present)
    self.assertEqual(x.py_value(), 'bar')
    with self.assertRaises(TypeError):
      x.__bool__()
    self.assertEqual(hash(x), hash('bar'))
    self.assertEqual(x.__str__(), "optional_text{'bar'}")
    self.assertEqual(rl_scalar_utils.py_text(x), 'bar')

  @parameterized.parameters(
      [rl_optional_qtype.optional_int32(None)],
      [rl_optional_qtype.optional_int64(None)],
  )
  def testOptionalInt_Missing(self, x):
    self.assertIsInstance(x, rl_optional_qvalue.OptionalInt)
    self.assertFalse(x.is_present)
    self.assertIsNone(x.py_value())
    with self.assertRaises(TypeError):
      x.__bool__()
    with self.assertRaises(rl_scalar_utils.MissingOptionalError):
      x.__index__()
    self.assertEqual(hash(x), hash(None))
    self.assertIsNone(rl_scalar_utils.py_index(x))

  @parameterized.parameters(
      [rl_optional_qtype.optional_int32(3)],
      [rl_optional_qtype.optional_int64(3)],
  )
  def testOptionalInt_Present(self, x):
    self.assertIsInstance(x, rl_optional_qvalue.OptionalInt)
    self.assertTrue(x.is_present)
    self.assertEqual(x.py_value(), 3)
    with self.assertRaises(TypeError):
      x.__bool__()
    self.assertEqual(x.__index__(), 3)
    self.assertEqual(hash(x), hash(3))
    self.assertEqual(rl_scalar_utils.py_index(x), 3)

  @parameterized.parameters(
      [rl_optional_qtype.optional_uint64(None)],
  )
  def testOptionalUInt64_Missing(self, x):
    self.assertIsInstance(x, rl_optional_qvalue.OptionalScalar)
    self.assertFalse(x.is_present)
    self.assertIsNone(x.py_value())
    with self.assertRaises(TypeError):
      x.__bool__()
    with self.assertRaises(rl_scalar_utils.MissingOptionalError):
      x.__index__()
    self.assertEqual(hash(x), hash(None))
    self.assertIsNone(rl_scalar_utils.py_index(x))

  @parameterized.parameters(
      [3, rl_optional_qtype.optional_uint64(3)],
      [2**64 - 1, rl_optional_qtype.optional_uint64(2**64 - 1)],
  )
  def testOptionalUInt64_Present(self, py_value, x):
    self.assertIsInstance(x, rl_optional_qvalue.OptionalScalar)
    self.assertTrue(x.is_present)
    self.assertEqual(x.py_value(), py_value)
    with self.assertRaises(TypeError):
      x.__bool__()
    self.assertEqual(x.__index__(), py_value)
    self.assertEqual(hash(x), hash(py_value))
    self.assertEqual(rl_scalar_utils.py_index(x), py_value)

  @parameterized.parameters(
      [rl_optional_qtype.optional_float32(None)],
      [rl_optional_qtype.optional_float64(None)],
  )
  def testOptionalFloat_Missing(self, x):
    self.assertIsInstance(x, rl_optional_qvalue.OptionalFloat)
    self.assertFalse(x.is_present)
    self.assertIsNone(x.py_value())
    with self.assertRaises(TypeError):
      x.__bool__()
    with self.assertRaises(rl_scalar_utils.MissingOptionalError):
      x.__int__()
    with self.assertRaises(rl_scalar_utils.MissingOptionalError):
      x.__float__()
    self.assertEqual(hash(x), hash(None))
    self.assertIsNone(rl_scalar_utils.py_float(x))

  @parameterized.parameters(
      [rl_optional_qtype.optional_float32(1.5)],
      [rl_optional_qtype.optional_float64(1.5)],
  )
  def testOptionalFloat_Present(self, x):
    self.assertIsInstance(x, rl_optional_qvalue.OptionalFloat)
    self.assertTrue(x.is_present)
    self.assertEqual(x.py_value(), 1.5)
    with self.assertRaises(TypeError):
      x.__bool__()
    self.assertEqual(x.__int__(), 1)
    self.assertEqual(x.__float__(), 1.5)
    self.assertEqual(hash(x), hash(1.5))
    self.assertEqual(rl_scalar_utils.py_float(x), 1.5)


if __name__ == '__main__':
  absltest.main()
