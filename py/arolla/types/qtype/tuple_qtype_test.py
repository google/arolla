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

"""Tests for arolla.types.qtype.tuple_qtype."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as rl_abc
from arolla.types.qtype import optional_qtype as rl_optional_qtype
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qtype import tuple_qtype as rl_tuple_qtype


_MAKE_TUPLE_QVALUE_CASES = (
    (),
    (rl_scalar_qtype.unit(),),
    (rl_scalar_qtype.unit(), rl_scalar_qtype.false()),
    (
        rl_scalar_qtype.unit(),
        rl_scalar_qtype.false(),
        rl_scalar_qtype.bytes_(b'foo'),
    ),
    (rl_tuple_qtype.make_tuple_qvalue()),
)


class TupleQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      *rl_scalar_qtype.SCALAR_QTYPES, *rl_optional_qtype.OPTIONAL_QTYPES
  )
  def testIsTupleQType_False(self, qtype):
    self.assertFalse(rl_tuple_qtype.is_tuple_qtype(qtype))

  @parameterized.parameters(*_MAKE_TUPLE_QVALUE_CASES)
  def testIsTupleQType_True(self, *args):
    self.assertTrue(
        rl_tuple_qtype.is_tuple_qtype(
            rl_tuple_qtype.make_tuple_qvalue(*args).qtype
        )
    )

  def testIsTupleQType_Error(self):
    with self.assertRaises(TypeError):
      rl_tuple_qtype.is_tuple_qtype(rl_scalar_qtype.int64(5))

  @parameterized.parameters(*_MAKE_TUPLE_QVALUE_CASES)
  def testMakeTupleQType(self, *args):
    self.assertEqual(
        rl_tuple_qtype.make_tuple_qvalue(*args).qtype,
        rl_tuple_qtype.make_tuple_qtype(*(arg.qtype for arg in args)),
    )

  def testMakeTupleQType_Error(self):
    with self.assertRaises(TypeError):
      rl_tuple_qtype.make_tuple_qtype(
          rl_scalar_qtype.INT32, rl_scalar_qtype.float64(5)
      )

  @parameterized.parameters(*_MAKE_TUPLE_QVALUE_CASES)
  def testFastGetNth(self, *args):
    tpl = rl_tuple_qtype.make_tuple_qvalue(*args)
    for i in range(len(args)):
      self.assertEqual(
          rl_tuple_qtype.get_nth(tpl, i).fingerprint,
          args[i].fingerprint,
      )

  def testFastGetNth_Error(self):
    tpl = rl_tuple_qtype.make_tuple_qvalue(
        rl_scalar_qtype.unit(), rl_scalar_qtype.false()
    )
    with self.assertRaises(IndexError):
      _ = rl_tuple_qtype.get_nth(tpl, -1)
      _ = rl_tuple_qtype.get_nth(tpl, 2)


_MAKE_NAMEDTUPLE_QTYPE_CASES = (
    {},
    dict(a=rl_scalar_qtype.UNIT),
    dict(a=rl_scalar_qtype.UNIT, b=rl_scalar_qtype.BOOLEAN),
    dict(
        a=rl_scalar_qtype.UNIT,
        b=rl_scalar_qtype.BOOLEAN,
        c=rl_scalar_qtype.BYTES,
    ),
    dict(a=rl_tuple_qtype.make_tuple_qvalue().qtype, b=rl_scalar_qtype.INT64),
)

_MAKE_NAMEDTUPLE_QVALUE_CASES = (
    {},
    dict(a=rl_scalar_qtype.unit()),
    dict(a=rl_scalar_qtype.unit(), b=rl_scalar_qtype.false()),
    dict(
        a=rl_scalar_qtype.unit(),
        b=rl_scalar_qtype.true(),
        c=rl_scalar_qtype.bytes_(b'abc'),
    ),
    dict(a=rl_tuple_qtype.make_tuple_qvalue(), b=rl_scalar_qtype.int64(0)),
)


class NamedTupleQTypeTest(parameterized.TestCase):

  @parameterized.parameters(
      *rl_scalar_qtype.SCALAR_QTYPES, *rl_optional_qtype.OPTIONAL_QTYPES
  )
  def testIsNamedTupleQType_False(self, qtype):
    self.assertFalse(rl_tuple_qtype.is_namedtuple_qtype(qtype))

  @parameterized.parameters(*_MAKE_NAMEDTUPLE_QTYPE_CASES)
  def testIsNamedTupleQType_True(self, **fields):
    self.assertTrue(
        rl_tuple_qtype.is_namedtuple_qtype(
            rl_tuple_qtype.make_namedtuple_qtype(**fields)
        )
    )

  def testNamedTupleQTypeNameAndFieldOrder(self):
    self.assertEqual(
        rl_tuple_qtype.make_namedtuple_qtype().name, 'namedtuple<>'
    )
    self.assertEqual(
        rl_tuple_qtype.make_namedtuple_qtype(
            a=rl_scalar_qtype.UNIT, b=rl_scalar_qtype.INT32
        ).name,
        'namedtuple<a=UNIT,b=INT32>',
    )
    self.assertEqual(
        rl_tuple_qtype.make_namedtuple_qtype(
            b=rl_scalar_qtype.UNIT, a=rl_scalar_qtype.INT32
        ).name,
        'namedtuple<b=UNIT,a=INT32>',
    )

  @parameterized.parameters(*_MAKE_NAMEDTUPLE_QTYPE_CASES)
  def testGetNamedTupleFieldQTypes(self, **fields):
    self.assertEqual(
        rl_tuple_qtype.get_namedtuple_field_qtypes(
            rl_tuple_qtype.make_namedtuple_qtype(**fields)
        ),
        fields,
    )

  def testGetNamedTupleFieldQType_Error(self):
    with self.assertRaisesRegex(TypeError, r'expected a namedtuple qtype, got'):
      rl_tuple_qtype.get_namedtuple_field_qtypes(rl_scalar_qtype.INT64)

  @parameterized.parameters(*_MAKE_NAMEDTUPLE_QTYPE_CASES)
  def testGetNamedTupleFieldIndex(self, **fields):
    named_tuple_qtype = rl_tuple_qtype.make_namedtuple_qtype(**fields)
    for i, field in enumerate(fields):
      self.assertEqual(
          rl_tuple_qtype.get_namedtuple_field_index(named_tuple_qtype, field),
          i,
      )
    self.assertIsNone(
        rl_tuple_qtype.get_namedtuple_field_index(
            named_tuple_qtype, 'unknown_field'
        )
    )

  def testIsTupleQType_Error(self):
    with self.assertRaises(TypeError):
      rl_tuple_qtype.is_namedtuple_qtype(rl_scalar_qtype.int64(5))

  def testMakeTupleQType_Error(self):
    with self.assertRaises(TypeError):
      rl_tuple_qtype.make_namedtuple_qtype(
          a=rl_scalar_qtype.INT32, b=rl_scalar_qtype.float64(5)
      )

  @parameterized.parameters(
      zip(_MAKE_NAMEDTUPLE_QTYPE_CASES, _MAKE_NAMEDTUPLE_QVALUE_CASES)
  )
  def testMakeNamedTupleQValue(self, types, values):
    qtype = rl_tuple_qtype.make_namedtuple_qtype(**types)
    qvalue = rl_tuple_qtype.make_namedtuple_qvalue(**values)
    self.assertEqual(qvalue.qtype, qtype)
    raw_tuple = rl_abc.eval_expr(
        rl_abc.bind_op('derived_qtype.upcast', qtype, qvalue)
    )
    self.assertEqual(
        raw_tuple.fingerprint,
        rl_tuple_qtype.make_tuple_qvalue(*values.values()).fingerprint,
    )


if __name__ == '__main__':
  absltest.main()
