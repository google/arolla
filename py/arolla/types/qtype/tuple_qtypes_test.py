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

"""Tests for arolla.types.qtype.tuple_qtypes."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.types.qtype import optional_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import tuple_qtypes


_MAKE_TUPLE_QVALUE_CASES = (
    (),
    (scalar_qtypes.unit(),),
    (scalar_qtypes.unit(), scalar_qtypes.false()),
    (
        scalar_qtypes.unit(),
        scalar_qtypes.false(),
        scalar_qtypes.bytes_(b'foo'),
    ),
    (tuple_qtypes.make_tuple_qvalue()),
)


class TupleQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      *scalar_qtypes.SCALAR_QTYPES, *optional_qtypes.OPTIONAL_QTYPES
  )
  def testIsTupleQType_False(self, qtype):
    self.assertFalse(tuple_qtypes.is_tuple_qtype(qtype))

  @parameterized.parameters(*_MAKE_TUPLE_QVALUE_CASES)
  def testIsTupleQType_True(self, *args):
    self.assertTrue(
        tuple_qtypes.is_tuple_qtype(tuple_qtypes.make_tuple_qvalue(*args).qtype)
    )

  def testIsTupleQType_Error(self):
    with self.assertRaises(TypeError):
      tuple_qtypes.is_tuple_qtype(scalar_qtypes.int64(5))

  @parameterized.parameters(*_MAKE_TUPLE_QVALUE_CASES)
  def testMakeTupleQType(self, *args):
    self.assertEqual(
        tuple_qtypes.make_tuple_qvalue(*args).qtype,
        tuple_qtypes.make_tuple_qtype(*(arg.qtype for arg in args)),
    )

  def testMakeTupleQType_Error(self):
    with self.assertRaises(TypeError):
      tuple_qtypes.make_tuple_qtype(
          scalar_qtypes.INT32, scalar_qtypes.float64(5)
      )

  @parameterized.parameters(*_MAKE_TUPLE_QVALUE_CASES)
  def testFastGetNth(self, *args):
    tpl = tuple_qtypes.make_tuple_qvalue(*args)
    for i in range(len(args)):
      self.assertEqual(
          tuple_qtypes.get_nth(tpl, i).fingerprint,
          args[i].fingerprint,
      )

  def testFastGetNth_Error(self):
    tpl = tuple_qtypes.make_tuple_qvalue(
        scalar_qtypes.unit(), scalar_qtypes.false()
    )
    with self.assertRaises(IndexError):
      _ = tuple_qtypes.get_nth(tpl, -1)
      _ = tuple_qtypes.get_nth(tpl, 2)


_MAKE_NAMEDTUPLE_QTYPE_CASES = (
    {},
    dict(a=scalar_qtypes.UNIT),
    dict(a=scalar_qtypes.UNIT, b=scalar_qtypes.BOOLEAN),
    dict(
        a=scalar_qtypes.UNIT,
        b=scalar_qtypes.BOOLEAN,
        c=scalar_qtypes.BYTES,
    ),
    dict(a=tuple_qtypes.make_tuple_qvalue().qtype, b=scalar_qtypes.INT64),
)

_MAKE_NAMEDTUPLE_QVALUE_CASES = (
    {},
    dict(a=scalar_qtypes.unit()),
    dict(a=scalar_qtypes.unit(), b=scalar_qtypes.false()),
    dict(
        a=scalar_qtypes.unit(),
        b=scalar_qtypes.true(),
        c=scalar_qtypes.bytes_(b'abc'),
    ),
    dict(a=tuple_qtypes.make_tuple_qvalue(), b=scalar_qtypes.int64(0)),
)


class NamedTupleQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      *scalar_qtypes.SCALAR_QTYPES, *optional_qtypes.OPTIONAL_QTYPES
  )
  def testIsNamedTupleQType_False(self, qtype):
    self.assertFalse(tuple_qtypes.is_namedtuple_qtype(qtype))

  @parameterized.parameters(*_MAKE_NAMEDTUPLE_QTYPE_CASES)
  def testIsNamedTupleQType_True(self, **fields):
    self.assertTrue(
        tuple_qtypes.is_namedtuple_qtype(
            tuple_qtypes.make_namedtuple_qtype(**fields)
        )
    )

  def testNamedTupleQTypeNameAndFieldOrder(self):
    self.assertEqual(tuple_qtypes.make_namedtuple_qtype().name, 'namedtuple<>')
    self.assertEqual(
        tuple_qtypes.make_namedtuple_qtype(
            a=scalar_qtypes.UNIT, b=scalar_qtypes.INT32
        ).name,
        'namedtuple<a=UNIT,b=INT32>',
    )
    self.assertEqual(
        tuple_qtypes.make_namedtuple_qtype(
            b=scalar_qtypes.UNIT, a=scalar_qtypes.INT32
        ).name,
        'namedtuple<b=UNIT,a=INT32>',
    )

  @parameterized.parameters(*_MAKE_NAMEDTUPLE_QTYPE_CASES)
  def testGetNamedTupleFieldQTypes(self, **fields):
    self.assertEqual(
        tuple_qtypes.get_namedtuple_field_qtypes(
            tuple_qtypes.make_namedtuple_qtype(**fields)
        ),
        fields,
    )

  def testGetNamedTupleFieldQType_Error(self):
    with self.assertRaisesRegex(TypeError, r'expected a namedtuple qtype, got'):
      tuple_qtypes.get_namedtuple_field_qtypes(scalar_qtypes.INT64)

  @parameterized.parameters(*_MAKE_NAMEDTUPLE_QTYPE_CASES)
  def testGetNamedTupleFieldIndex(self, **fields):
    named_tuple_qtype = tuple_qtypes.make_namedtuple_qtype(**fields)
    for i, field in enumerate(fields):
      self.assertEqual(
          tuple_qtypes.get_namedtuple_field_index(named_tuple_qtype, field),
          i,
      )
    self.assertIsNone(
        tuple_qtypes.get_namedtuple_field_index(
            named_tuple_qtype, 'unknown_field'
        )
    )

  def testIsTupleQType_Error(self):
    with self.assertRaises(TypeError):
      tuple_qtypes.is_namedtuple_qtype(scalar_qtypes.int64(5))

  def testMakeTupleQType_Error(self):
    with self.assertRaises(TypeError):
      tuple_qtypes.make_namedtuple_qtype(
          a=scalar_qtypes.INT32, b=scalar_qtypes.float64(5)
      )

  @parameterized.parameters(
      zip(_MAKE_NAMEDTUPLE_QTYPE_CASES, _MAKE_NAMEDTUPLE_QVALUE_CASES)
  )
  def testMakeNamedTupleQValue(self, types, values):
    qtype = tuple_qtypes.make_namedtuple_qtype(**types)
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    self.assertEqual(qvalue.qtype, qtype)
    raw_tuple = arolla_abc.eval_expr(
        arolla_abc.bind_op('derived_qtype.upcast', qtype, qvalue)
    )
    self.assertEqual(
        raw_tuple.fingerprint,
        tuple_qtypes.make_tuple_qvalue(*values.values()).fingerprint,
    )


if __name__ == '__main__':
  absltest.main()
