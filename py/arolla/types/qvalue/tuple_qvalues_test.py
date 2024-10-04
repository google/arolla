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

"""Tests for arolla.types.qvalue.tuple_qvalues."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.testing import testing as arolla_testing
from arolla.types.qtype import scalar_qtypes
from arolla.types.qtype import tuple_qtypes
from arolla.types.qvalue import scalar_qvalues as _
from arolla.types.qvalue import tuple_qvalues

_MAKE_TUPLE_CASES = (
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

  @parameterized.parameters(*_MAKE_TUPLE_CASES)
  def testTuple_Type(self, *args):
    qvalue = tuple_qtypes.make_tuple_qvalue(*args)
    self.assertIsInstance(qvalue, tuple_qvalues.Tuple)

  @parameterized.parameters(*_MAKE_TUPLE_CASES)
  def testTuple_Len(self, *args):
    qvalue = tuple_qtypes.make_tuple_qvalue(*args)
    self.assertLen(qvalue, len(args))

  @parameterized.parameters(*_MAKE_TUPLE_CASES)
  def testTuple_GetItem(self, *args):
    qvalue = tuple_qtypes.make_tuple_qvalue(*args)
    for i in range(-len(args), len(args)):
      self.assertEqual(qvalue[i].fingerprint, args[i].fingerprint)

  def testTuple_GetItem_Error(self):
    qvalue = tuple_qtypes.make_tuple_qvalue(
        scalar_qtypes.int32(1), scalar_qtypes.int32(2)
    )
    with self.assertRaisesRegex(IndexError, 'index out of range: -100'):
      _ = qvalue[-100]

  def testTuple_GetSlice(self):
    n = 5
    value = range(n)
    qvalue = tuple_qtypes.make_tuple_qvalue(*map(scalar_qtypes.int32, value))
    for start in [None, *range(-n, n)]:
      for stop in [None, *range(-n, n)]:
        for step in [None, *range(-n, 0), *range(1, 10)]:
          actual_result = qvalue[start:stop:step]
          expected_result = tuple_qtypes.make_tuple_qvalue(
              *map(scalar_qtypes.int32, value[start:stop:step])
          )
          arolla_testing.assert_qvalue_equal_by_fingerprint(
              actual_result, expected_result
          )

  def testTuple_GetSlice_Error(self):
    qvalue = tuple_qtypes.make_tuple_qvalue(
        scalar_qtypes.int32(0), scalar_qtypes.int32(1), scalar_qtypes.int32(2)
    )
    with self.assertRaisesRegex(ValueError, 'slice step cannot be zero'):
      _ = qvalue[::0]
    with self.assertRaisesRegex(TypeError, 'slice indices must be integers'):
      _ = qvalue['foo'::]

  @parameterized.parameters(*_MAKE_TUPLE_CASES)
  def testTupleQValueSpecialisation_PyValue(self, *args):
    qvalue = tuple_qtypes.make_tuple_qvalue(*args)
    self.assertEqual(qvalue.py_value(), tuple(arg.py_value() for arg in args))

  def testTupleQValueSpecialisation_Repr(self):
    self.assertEqual(repr(tuple_qtypes.make_tuple_qvalue()), '()')
    self.assertEqual(
        repr(
            tuple_qtypes.make_tuple_qvalue(
                scalar_qtypes.int32(1),
                scalar_qtypes.int64(2),
                scalar_qtypes.float64(3.0),
            )
        ),
        '(1, int64{2}, float64{3})',
    )


_MAKE_NAMED_TUPLE_CASES = (
    {},
    dict(a=scalar_qtypes.unit()),
    dict(a=scalar_qtypes.unit(), b=scalar_qtypes.false()),
    dict(
        a=scalar_qtypes.unit(),
        b=scalar_qtypes.false(),
        c=scalar_qtypes.bytes_(b'foo'),
    ),
    dict(x=tuple_qtypes.make_tuple_qvalue()),
)


class NamedTupleQTypeTest(parameterized.TestCase):

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_Type(self, **values):
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    self.assertIsInstance(qvalue, tuple_qvalues.NamedTuple)

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_GetItem(self, **values):
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    for f in values:
      self.assertEqual(qvalue[f].fingerprint, values[f].fingerprint)

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_Contains(self, **values):
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    for f in values:
      self.assertIn(f, qvalue)

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTupleKeys(self, **values):
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    self.assertEqual(qvalue.keys(), list(values.keys()))

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_Contains_False(self, **values):
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    self.assertNotIn('unknown', qvalue)

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTuple_GetItem_Error(self, **values):
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    with self.assertRaisesRegex(KeyError, '.*unknown.*not found.*'):
      _ = qvalue['unknown']

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTupleQValueSpecialisation_AsDict(self, **values):
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    qvalue_as_dict = qvalue.as_dict()
    self.assertIsInstance(qvalue_as_dict, dict)
    self.assertEqual(qvalue_as_dict.keys(), values.keys())
    for actual_value, expected_value in zip(
        qvalue_as_dict.values(), values.values()
    ):
      arolla_testing.assert_qvalue_equal_by_fingerprint(
          actual_value, expected_value
      )

  @parameterized.parameters(*_MAKE_NAMED_TUPLE_CASES)
  def testTupleQValueSpecialisation_PyValue(self, **values):
    qvalue = tuple_qtypes.make_namedtuple_qvalue(**values)
    py_value = qvalue.py_value()
    self.assertIsInstance(py_value, tuple)
    self.assertDictEqual(
        py_value._asdict(), {k: v.py_value() for k, v in values.items()}  # pytype: disable=attribute-error
    )

  def testTupleQValueSpecialisation_Repr(self):
    self.assertEqual(
        repr(tuple_qtypes.make_namedtuple_qvalue()), 'namedtuple<>{()}'
    )
    self.assertEqual(
        repr(
            tuple_qtypes.make_namedtuple_qvalue(
                a=scalar_qtypes.unit(), b=scalar_qtypes.false()
            )
        ),
        'namedtuple<a=UNIT,b=BOOLEAN>{(unit, false)}',
    )

  def testCacheClear(self):
    x = tuple_qtypes.make_namedtuple_qvalue()
    x.py_value()
    self.assertGreater(
        tuple_qvalues._named_tuple_cls_from_qtype.cache_info().currsize, 0
    )
    arolla_abc.clear_caches()
    self.assertEqual(
        tuple_qvalues._named_tuple_cls_from_qtype.cache_info().currsize, 0
    )


if __name__ == '__main__':
  absltest.main()
