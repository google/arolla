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

"""Tests for arolla.types.qvalue.scalar_qvalue."""

# TODO: Tests for int -> float overflow (like 10**1000)

from absl.testing import absltest
from absl.testing import parameterized
from arolla.operators import operators_clib as _
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qtype import scalar_utils as rl_scalar_utils
from arolla.types.qvalue import optional_qvalue as _
from arolla.types.qvalue import qvalue_mixins as rl_qvalue_mixins
from arolla.types.qvalue import scalar_qvalue as rl_scalar_qvalue


class ScalarTest(parameterized.TestCase):

  def testInheritance(self):
    self.assertTrue(
        issubclass(
            rl_scalar_qvalue.Scalar, rl_qvalue_mixins.PresenceQValueMixin
        )
    )
    self.assertTrue(issubclass(rl_scalar_qvalue.Unit, rl_scalar_qvalue.Scalar))
    self.assertTrue(
        issubclass(rl_scalar_qvalue.Boolean, rl_scalar_qvalue.Scalar)
    )
    self.assertTrue(issubclass(rl_scalar_qvalue.Bytes, rl_scalar_qvalue.Scalar))
    self.assertTrue(issubclass(rl_scalar_qvalue.Text, rl_scalar_qvalue.Scalar))
    self.assertTrue(issubclass(rl_scalar_qvalue.Int, rl_scalar_qvalue.Scalar))
    self.assertTrue(
        issubclass(
            rl_scalar_qvalue.Int, rl_qvalue_mixins.IntegralArithmeticQValueMixin
        )
    )
    self.assertTrue(issubclass(rl_scalar_qvalue.Float, rl_scalar_qvalue.Scalar))
    self.assertTrue(
        issubclass(
            rl_scalar_qvalue.Float,
            rl_qvalue_mixins.FloatingPointArithmeticQValueMixin,
        )
    )

  def testQValueSpecialization_Unit(self):
    x = rl_scalar_qtype.unit()
    self.assertIsInstance(x, rl_scalar_qvalue.Unit)
    self.assertIs(x.py_value(), True)
    self.assertEqual(hash(x), hash(True))
    self.assertEqual(repr(x), 'unit')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Boolean_False(self):
    x = rl_scalar_qtype.false()
    self.assertIsInstance(x, rl_scalar_qvalue.Boolean)
    self.assertIs(x.py_value(), False)
    self.assertFalse(x.__bool__())
    self.assertEqual(hash(x), hash(False))
    self.assertEqual(rl_scalar_utils.py_boolean(x), False)

  def testQValueSpecialization_Boolean_True(self):
    x = rl_scalar_qtype.true()
    self.assertIsInstance(x, rl_scalar_qvalue.Boolean)
    self.assertIs(x.py_value(), True)
    self.assertTrue(x.__bool__())
    self.assertEqual(hash(x), hash(True))
    self.assertEqual(rl_scalar_utils.py_boolean(x), True)

  def testQValueSpecialization_Bytes_Empty(self):
    x = rl_scalar_qtype.bytes_(b'')
    self.assertIsInstance(x, rl_scalar_qvalue.Bytes)
    self.assertEqual(x.py_value(), b'')
    self.assertEqual(x.__bytes__(), b'')
    self.assertEqual(hash(x), hash(b''))
    self.assertEqual(rl_scalar_utils.py_bytes(x), b'')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Bytes_Foo(self):
    x = rl_scalar_qtype.bytes_(b'foo')
    self.assertIsInstance(x, rl_scalar_qvalue.Bytes)
    self.assertEqual(x.py_value(), b'foo')
    self.assertEqual(x.__bytes__(), b'foo')
    self.assertEqual(hash(x), hash(b'foo'))
    self.assertEqual(rl_scalar_utils.py_bytes(x), b'foo')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Text_Empty(self):
    x = rl_scalar_qtype.text('')
    self.assertIsInstance(x, rl_scalar_qvalue.Text)
    self.assertEqual(x.py_value(), '')
    self.assertEqual(hash(x), hash(''))
    self.assertEqual(x.__str__(), "''")
    self.assertEqual(rl_scalar_utils.py_text(x), '')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Text_Bar(self):
    x = rl_scalar_qtype.text('bar')
    self.assertIsInstance(x, rl_scalar_qvalue.Text)
    self.assertEqual(x.py_value(), 'bar')
    self.assertEqual(hash(x), hash('bar'))
    self.assertEqual(x.__str__(), "'bar'")
    self.assertEqual(rl_scalar_utils.py_text(x), 'bar')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Int_0(self):
    x = rl_scalar_qtype.int32(0)
    self.assertIsInstance(x, rl_scalar_qvalue.Int)
    self.assertEqual(x.py_value(), 0)
    self.assertEqual(x.__index__(), 0)
    self.assertEqual(hash(x), hash(0))
    self.assertEqual(rl_scalar_utils.py_index(x), 0)
    self.assertEqual(rl_scalar_utils.py_float(x), 0.0)
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Int_1(self):
    x = rl_scalar_qtype.int64(1)
    self.assertIsInstance(x, rl_scalar_qvalue.Int)
    self.assertEqual(x.py_value(), 1)
    self.assertEqual(x.__index__(), 1)
    self.assertEqual(hash(x), hash(1))
    self.assertEqual(rl_scalar_utils.py_index(x), 1)
    self.assertEqual(rl_scalar_utils.py_float(x), 1.0)
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Float_0(self):
    x = rl_scalar_qtype.float32(0.0)
    self.assertIsInstance(x, rl_scalar_qvalue.Float)
    self.assertEqual(x.py_value(), 0)
    self.assertEqual(x.__int__(), 0)
    self.assertEqual(x.__float__(), 0.0)
    self.assertEqual(hash(x), hash(0.0))
    self.assertEqual(rl_scalar_utils.py_float(x), 0.0)
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Float_1_5(self):
    x = rl_scalar_qtype.float64(1.5)
    self.assertIsInstance(x, rl_scalar_qvalue.Float)
    self.assertEqual(x.py_value(), 1.5)
    self.assertEqual(x.__int__(), 1)
    self.assertEqual(x.__float__(), 1.5)
    self.assertEqual(hash(x), hash(1.5))
    self.assertEqual(rl_scalar_utils.py_float(x), 1.5)
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_UInt64(self):
    x = rl_scalar_qtype.uint64(1)
    self.assertIsInstance(x, rl_scalar_qvalue.UInt)
    self.assertEqual(x.py_value(), 1)
    self.assertEqual(x.__index__(), 1)
    self.assertEqual(hash(x), hash(1))
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_UInt64_ExtremeValue(self):
    x = rl_scalar_qtype.uint64(2**64 - 1)
    self.assertIsInstance(x, rl_scalar_qvalue.Scalar)
    self.assertEqual(x.py_value(), 2**64 - 1)
    self.assertEqual(hash(x), hash(2**64 - 1))

  def testQValueSpecialization_UInt64_OutOfRange(self):
    with self.assertRaises(OverflowError):
      rl_scalar_qtype.uint64(2**64)
    with self.assertRaises(OverflowError):
      rl_scalar_qtype.uint64(-1)

  def testIndexByQNumeric(self):
    vals = [-1, 0, 1]
    self.assertEqual(vals[rl_scalar_qtype.int32(-1)], 1)
    self.assertEqual(vals[rl_scalar_qtype.int64(0)], -1)
    self.assertEqual(vals[rl_scalar_qtype.int32(1)], 0)
    with self.assertRaises(TypeError):
      _ = vals[rl_scalar_qtype.float32(0.0)]

  def testTrue(self):
    self.assertEqual(rl_scalar_qtype.true().qtype, rl_scalar_qtype.BOOLEAN)
    self.assertEqual(rl_scalar_qtype.true(), True)

  def testFalse(self):
    self.assertEqual(rl_scalar_qtype.false().qtype, rl_scalar_qtype.BOOLEAN)
    self.assertEqual(rl_scalar_qtype.false(), False)

  def testScalarShape(self):
    shape = rl_scalar_qvalue.ScalarShape()
    self.assertIsInstance(shape, rl_scalar_qvalue.ScalarShape)
    self.assertEqual(shape.qtype, rl_scalar_qtype.SCALAR_SHAPE)

  def testScalarToScalarEdge(self):
    edge = rl_scalar_qvalue.ScalarToScalarEdge()
    self.assertIsInstance(edge, rl_scalar_qvalue.ScalarToScalarEdge)
    self.assertEqual(edge.qtype, rl_scalar_qtype.SCALAR_TO_SCALAR_EDGE)


if __name__ == '__main__':
  absltest.main()
