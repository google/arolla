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

"""Tests for arolla.types.qvalue.scalar_qvalues."""

# TODO: Tests for int -> float overflow (like 10**1000)

from absl.testing import absltest
from absl.testing import parameterized
from arolla.operators import operators_clib as _
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import scalar_utils
from arolla.types.qvalue import optional_qvalues as _
from arolla.types.qvalue import qvalue_mixins
from arolla.types.qvalue import scalar_qvalues


class ScalarTest(parameterized.TestCase):

  def testInheritance(self):
    self.assertTrue(
        issubclass(scalar_qvalues.Scalar, qvalue_mixins.PresenceQValueMixin)
    )
    self.assertTrue(issubclass(scalar_qvalues.Unit, scalar_qvalues.Scalar))
    self.assertTrue(issubclass(scalar_qvalues.Boolean, scalar_qvalues.Scalar))
    self.assertTrue(issubclass(scalar_qvalues.Bytes, scalar_qvalues.Scalar))
    self.assertTrue(issubclass(scalar_qvalues.Text, scalar_qvalues.Scalar))
    self.assertTrue(issubclass(scalar_qvalues.Int, scalar_qvalues.Scalar))
    self.assertTrue(
        issubclass(
            scalar_qvalues.Int, qvalue_mixins.IntegralArithmeticQValueMixin
        )
    )
    self.assertTrue(issubclass(scalar_qvalues.Float, scalar_qvalues.Scalar))
    self.assertTrue(
        issubclass(
            scalar_qvalues.Float,
            qvalue_mixins.FloatingPointArithmeticQValueMixin,
        )
    )

  def testQValueSpecialization_Unit(self):
    x = scalar_qtypes.unit()
    self.assertIsInstance(x, scalar_qvalues.Unit)
    self.assertIs(x.py_value(), True)
    self.assertEqual(hash(x), hash(True))
    self.assertEqual(repr(x), 'unit')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Boolean_False(self):
    x = scalar_qtypes.false()
    self.assertIsInstance(x, scalar_qvalues.Boolean)
    self.assertIs(x.py_value(), False)
    self.assertFalse(x.__bool__())
    self.assertEqual(hash(x), hash(False))
    self.assertEqual(scalar_utils.py_boolean(x), False)

  def testQValueSpecialization_Boolean_True(self):
    x = scalar_qtypes.true()
    self.assertIsInstance(x, scalar_qvalues.Boolean)
    self.assertIs(x.py_value(), True)
    self.assertTrue(x.__bool__())
    self.assertEqual(hash(x), hash(True))
    self.assertEqual(scalar_utils.py_boolean(x), True)

  def testQValueSpecialization_Bytes_Empty(self):
    x = scalar_qtypes.bytes_(b'')
    self.assertIsInstance(x, scalar_qvalues.Bytes)
    self.assertEqual(x.py_value(), b'')
    self.assertEqual(x.__bytes__(), b'')
    self.assertEqual(hash(x), hash(b''))
    self.assertEqual(scalar_utils.py_bytes(x), b'')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Bytes_Foo(self):
    x = scalar_qtypes.bytes_(b'foo')
    self.assertIsInstance(x, scalar_qvalues.Bytes)
    self.assertEqual(x.py_value(), b'foo')
    self.assertEqual(x.__bytes__(), b'foo')
    self.assertEqual(hash(x), hash(b'foo'))
    self.assertEqual(scalar_utils.py_bytes(x), b'foo')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Text_Empty(self):
    x = scalar_qtypes.text('')
    self.assertIsInstance(x, scalar_qvalues.Text)
    self.assertEqual(x.py_value(), '')
    self.assertEqual(hash(x), hash(''))
    self.assertEqual(x.__str__(), "''")
    self.assertEqual(scalar_utils.py_text(x), '')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Text_Bar(self):
    x = scalar_qtypes.text('bar')
    self.assertIsInstance(x, scalar_qvalues.Text)
    self.assertEqual(x.py_value(), 'bar')
    self.assertEqual(hash(x), hash('bar'))
    self.assertEqual(x.__str__(), "'bar'")
    self.assertEqual(scalar_utils.py_text(x), 'bar')
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Int_0(self):
    x = scalar_qtypes.int32(0)
    self.assertIsInstance(x, scalar_qvalues.Int)
    self.assertEqual(x.py_value(), 0)
    self.assertEqual(x.__index__(), 0)
    self.assertEqual(hash(x), hash(0))
    self.assertEqual(scalar_utils.py_index(x), 0)
    self.assertEqual(scalar_utils.py_float(x), 0.0)
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Int_1(self):
    x = scalar_qtypes.int64(1)
    self.assertIsInstance(x, scalar_qvalues.Int)
    self.assertEqual(x.py_value(), 1)
    self.assertEqual(x.__index__(), 1)
    self.assertEqual(hash(x), hash(1))
    self.assertEqual(scalar_utils.py_index(x), 1)
    self.assertEqual(scalar_utils.py_float(x), 1.0)
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Float_0(self):
    x = scalar_qtypes.float32(0.0)
    self.assertIsInstance(x, scalar_qvalues.Float)
    self.assertEqual(x.py_value(), 0)
    self.assertEqual(x.__int__(), 0)
    self.assertEqual(x.__float__(), 0.0)
    self.assertEqual(hash(x), hash(0.0))
    self.assertEqual(scalar_utils.py_float(x), 0.0)
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_Float_1_5(self):
    x = scalar_qtypes.float64(1.5)
    self.assertIsInstance(x, scalar_qvalues.Float)
    self.assertEqual(x.py_value(), 1.5)
    self.assertEqual(x.__int__(), 1)
    self.assertEqual(x.__float__(), 1.5)
    self.assertEqual(hash(x), hash(1.5))
    self.assertEqual(scalar_utils.py_float(x), 1.5)
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_UInt64(self):
    x = scalar_qtypes.uint64(1)
    self.assertIsInstance(x, scalar_qvalues.UInt)
    self.assertEqual(x.py_value(), 1)
    self.assertEqual(x.__index__(), 1)
    self.assertEqual(hash(x), hash(1))
    with self.assertRaises(TypeError):
      x.__bool__()

  def testQValueSpecialization_UInt64_ExtremeValue(self):
    x = scalar_qtypes.uint64(2**64 - 1)
    self.assertIsInstance(x, scalar_qvalues.Scalar)
    self.assertEqual(x.py_value(), 2**64 - 1)
    self.assertEqual(hash(x), hash(2**64 - 1))

  def testQValueSpecialization_UInt64_OutOfRange(self):
    with self.assertRaises(OverflowError):
      scalar_qtypes.uint64(2**64)
    with self.assertRaises(OverflowError):
      scalar_qtypes.uint64(-1)

  def testIndexByQNumeric(self):
    vals = [-1, 0, 1]
    self.assertEqual(vals[scalar_qtypes.int32(-1)], 1)
    self.assertEqual(vals[scalar_qtypes.int64(0)], -1)
    self.assertEqual(vals[scalar_qtypes.int32(1)], 0)
    with self.assertRaises(TypeError):
      _ = vals[scalar_qtypes.float32(0.0)]

  def testTrue(self):
    self.assertEqual(scalar_qtypes.true().qtype, scalar_qtypes.BOOLEAN)
    self.assertEqual(scalar_qtypes.true(), True)

  def testFalse(self):
    self.assertEqual(scalar_qtypes.false().qtype, scalar_qtypes.BOOLEAN)
    self.assertEqual(scalar_qtypes.false(), False)

  def testScalarShape(self):
    shape = scalar_qvalues.ScalarShape()
    self.assertIsInstance(shape, scalar_qvalues.ScalarShape)
    self.assertEqual(shape.qtype, scalar_qtypes.SCALAR_SHAPE)

  def testScalarToScalarEdge(self):
    edge = scalar_qvalues.ScalarToScalarEdge()
    self.assertIsInstance(edge, scalar_qvalues.ScalarToScalarEdge)
    self.assertEqual(edge.qtype, scalar_qtypes.SCALAR_TO_SCALAR_EDGE)


if __name__ == '__main__':
  absltest.main()
