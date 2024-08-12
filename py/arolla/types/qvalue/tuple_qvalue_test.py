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

"""Tests for arolla.types.qvalue.tuple_qvalue."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qtype import scalar_qtype as rl_scalar_qtype
from arolla.types.qtype import tuple_qtype as rl_tuple_qtype
from arolla.types.qvalue import scalar_qvalue as _
from arolla.types.qvalue import tuple_qvalue as rl_tuple_qvalue

_MAKE_TUPLE_CASES = (
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

  @parameterized.parameters(*_MAKE_TUPLE_CASES)
  def testTuple_Type(self, *args):
    qvalue = rl_tuple_qtype.make_tuple_qvalue(*args)
    self.assertIsInstance(qvalue, rl_tuple_qvalue.Tuple)

  @parameterized.parameters(*_MAKE_TUPLE_CASES)
  def testTuple_Len(self, *args):
    qvalue = rl_tuple_qtype.make_tuple_qvalue(*args)
    self.assertLen(qvalue, len(args))

  @parameterized.parameters(*_MAKE_TUPLE_CASES)
  def testTuple_GetItem(self, *args):
    qvalue = rl_tuple_qtype.make_tuple_qvalue(*args)
    for i in range(-len(args), len(args)):
      self.assertEqual(qvalue[i].fingerprint, args[i].fingerprint)

  def testTuple_GetItem_Error(self):
    qvalue = rl_tuple_qtype.make_tuple_qvalue(
        rl_scalar_qtype.int32(1), rl_scalar_qtype.int32(2)
    )
    with self.assertRaisesRegex(IndexError, 'index out of range: -100'):
      _ = qvalue[-100]

  @parameterized.parameters(*_MAKE_TUPLE_CASES)
  def testTupleQValueSpecialisation_PyValue(self, *args):
    qvalue = rl_tuple_qtype.make_tuple_qvalue(*args)
    self.assertEqual(qvalue.py_value(), tuple(arg.py_value() for arg in args))

  def testTupleQValueSpecialisation_Repr(self):
    self.assertEqual(repr(rl_tuple_qtype.make_tuple_qvalue()), '()')
    self.assertEqual(
        repr(
            rl_tuple_qtype.make_tuple_qvalue(
                rl_scalar_qtype.int32(1),
                rl_scalar_qtype.int64(2),
                rl_scalar_qtype.float64(3.0),
            )
        ),
        '(1, int64{2}, float64{3})',
    )


_MAKE_NAMED_TUPLE_CASES = (
    {},
    dict(a=rl_scalar_qtype.unit()),
    dict(a=rl_scalar_qtype.unit(), b=rl_scalar_qtype.false()),
    dict(
        a=rl_scalar_qtype.unit(),
        b=rl_scalar_qtype.false(),
        c=rl_scalar_qtype.bytes_(b'foo'),
    ),
    dict(x=rl_tuple_qtype.make_tuple_qvalue()),
)


class NamedTupleQTypeTest(parameterized.TestCase):

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_Type(self, **values):
    qvalue = rl_tuple_qtype.make_namedtuple_qvalue(**values)
    self.assertIsInstance(qvalue, rl_tuple_qvalue.NamedTuple)

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_GetItem(self, **values):
    qvalue = rl_tuple_qtype.make_namedtuple_qvalue(**values)
    for f in values:
      self.assertEqual(qvalue[f].fingerprint, values[f].fingerprint)

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_Contains(self, **values):
    qvalue = rl_tuple_qtype.make_namedtuple_qvalue(**values)
    for f in values:
      self.assertIn(f, qvalue)

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTupleKeys(self, **values):
    qvalue = rl_tuple_qtype.make_namedtuple_qvalue(**values)
    self.assertEqual(qvalue.keys(), list(values.keys()))

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_Contains_False(self, **values):
    qvalue = rl_tuple_qtype.make_namedtuple_qvalue(**values)
    self.assertNotIn('unknown', qvalue)

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_GetItem_Error(self, **values):
    qvalue = rl_tuple_qtype.make_namedtuple_qvalue(**values)
    with self.assertRaisesRegex(KeyError, '.*unknown.*not found.*'):
      _ = qvalue['unknown']

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTupleQValueSpecialisation_PyValue(self, **values):
    qvalue = rl_tuple_qtype.make_namedtuple_qvalue(**values)
    py_value = qvalue.py_value()
    self.assertIsInstance(py_value, tuple)
    self.assertDictEqual(
        py_value._asdict(), {k: v.py_value() for k, v in values.items()}  # pytype: disable=attribute-error
    )

  def testTupleQValueSpecialisation_Repr(self):
    self.assertEqual(
        repr(rl_tuple_qtype.make_namedtuple_qvalue()), 'namedtuple<>{()}'
    )
    self.assertEqual(
        repr(
            rl_tuple_qtype.make_namedtuple_qvalue(
                a=rl_scalar_qtype.unit(), b=rl_scalar_qtype.false()
            )
        ),
        'namedtuple<a=UNIT,b=BOOLEAN>{(unit, false)}',
    )

  def testCacheClear(self):
    x = rl_tuple_qtype.make_namedtuple_qvalue()
    x.py_value()
    self.assertGreater(
        rl_tuple_qvalue._named_tuple_cls_from_qtype.cache_info().currsize, 0
    )
    arolla_abc.clear_caches()
    self.assertEqual(
        rl_tuple_qvalue._named_tuple_cls_from_qtype.cache_info().currsize, 0
    )


if __name__ == '__main__':
  absltest.main()
