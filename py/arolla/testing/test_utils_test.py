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

from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.expr import expr as arolla_expr
from arolla.operators import operators_clib as _
from arolla.testing import test_utils
from arolla.types import types as arolla_types

L = arolla_expr.LeafContainer()
M = arolla_expr.OperatorsContainer()

NAN = float('nan')
INF = float('inf')


class TestUtilTest(parameterized.TestCase):

  @parameterized.parameters(
      (None, None, None),
      (None, NAN, 'not close: actual=None, expected=nan'),
      (None, 0.25, 'not close: actual=None, expected=0.25'),
      (None, INF, 'not close: actual=None, expected=inf'),
      (NAN, None, 'not close: actual=nan, expected=None'),
      (NAN, NAN, None),
      (NAN, 0.25, 'not close: actual=nan, expected=0.25'),
      (NAN, INF, 'not close: actual=nan, expected=inf'),
      (0.25, None, 'not close: actual=0.25, expected=None'),
      (0.25, NAN, 'not close: actual=0.25, expected=nan'),
      (0.25, 0.25, None),
      (0.25, INF, 'not close: actual=0.25, expected=inf'),
      (0.5, 0.25, 'not close: actual=0.5, expected=0.25, dif=0.25, tol=0.125'),
      (0.5, 0.25, 'not close: actual=0.5, expected=0.25, dif=0.25, tol=0.125'),
      (INF, None, 'not close: actual=inf, expected=None'),
      (INF, NAN, 'not close: actual=inf, expected=nan'),
      (INF, 0.25, 'not close: actual=inf, expected=0.25'),
      (INF, INF, None),
      (INF, -INF, 'not close: actual=inf, expected=-inf'),
      (-INF, INF, 'not close: actual=-inf, expected=inf'),
      (-INF, -INF, None),
  )
  def testPyFloatIsClose(self, lhs, rhs, error):
    check_fn = test_utils.PyFloatIsCloseCheck(rtol=0.1, atol=0.1)
    self.assertEqual(check_fn(lhs, rhs), error)

  @parameterized.parameters(
      (1.0, 'bar'),
      ('bar', 'bar'),
      ('bar', 1.0),
  )
  def testPyFloatIsClose_TypeError(self, lhs, rhs):
    check_fn = test_utils.PyFloatIsCloseCheck(rtol=0.1, atol=0.1)
    with self.assertRaises(TypeError):
      check_fn(lhs, rhs)

  @parameterized.parameters(
      (None, None, None),
      (None, NAN, 'not equal: actual=None, expected=nan'),
      (None, 'bar', "not equal: actual=None, expected='bar'"),
      (NAN, None, 'not equal: actual=nan, expected=None'),
      (NAN, NAN, None),
      (NAN, 'bar', "not equal: actual=nan, expected='bar'"),
      ('bar', None, "not equal: actual='bar', expected=None"),
      ('bar', NAN, "not equal: actual='bar', expected=nan"),
      ('bar', 'bar', None),
      ('bar', 'foo', "not equal: actual='bar', expected='foo'"),
      (0.0, 0.0, None),
      (0.0, -0.0, 'not equal: actual=0.0, expected=-0.0'),
      (NAN, -NAN, None),
  )
  def testPyObjectIsEqual(self, lhs, rhs, error):
    check_fn = test_utils.PyObjectIsEqualCheck()
    self.assertEqual(check_fn(lhs, rhs), error)

  @parameterized.parameters(
      arolla_types.int64(1),
      arolla_types.float32(0.0),
      arolla_types.float32(NAN),
  )
  def testQValueIsEqualByFingerprint_ok(self, x):
    check_fn = test_utils.QValueIsEqualByFingerprintCheck()
    self.assertIsNone(check_fn(x, x))

  @parameterized.parameters(
      (arolla_types.int64(1), arolla_types.int32(1)),
      (arolla_types.float32(NAN), arolla_types.float32(-NAN)),
      (arolla_types.float32(0.0), arolla_types.float32(-0.0)),
  )
  def testQValueIsEqualByFingerprint_error(self, lhs, rhs):
    check_fn = test_utils.QValueIsEqualByFingerprintCheck()
    error = check_fn(lhs, rhs)
    # The assertIsNotNone() is unnecessary, but it generates a better message
    # if `error` is None.
    self.assertIsNotNone(error)
    self.assertRegex(
        error, 'not equal by fingerprint: actual=[0-9a-f]*, expected=[0-9a-f]*'
    )

  def test_make_allclose_check_predicate(self):
    with self.subTest('float32, rtol=val, atol=val'):
      check_fn = test_utils._make_allclose_check_predicate(
          arolla_types.FLOAT32, rtol=0, atol=1
      )
      self.assertIsInstance(check_fn, test_utils.PyFloatIsCloseCheck)
      self.assertEqual(check_fn._rtol, 0)
      self.assertEqual(check_fn._atol, 1)
    with self.subTest('float32, rtol=dict, atol=dict'):
      check_fn = test_utils._make_allclose_check_predicate(
          arolla_types.FLOAT32,
          rtol={
              arolla_types.FLOAT32: 1,
              arolla_types.FLOAT64: 2,
          },
          atol={
              arolla_types.FLOAT32: 3,
              arolla_types.FLOAT64: 4,
          },
      )
      self.assertIsInstance(check_fn, test_utils.PyFloatIsCloseCheck)
      self.assertEqual(check_fn._rtol, 1)
      self.assertEqual(check_fn._atol, 3)
    with self.subTest('int32'):
      check_fn = test_utils._make_allclose_check_predicate(
          arolla_types.INT32, rtol=1, atol=1
      )
      self.assertIsInstance(check_fn, test_utils.PyObjectIsEqualCheck)
    with self.subTest('missing_rtol'):
      with self.assertRaises(KeyError):
        _ = test_utils._make_allclose_check_predicate(
            arolla_types.FLOAT32, rtol={}, atol=1
        )
    with self.subTest('missing_atol'):
      with self.assertRaises(KeyError):
        _ = test_utils._make_allclose_check_predicate(
            arolla_types.FLOAT32, rtol=1, atol={}
        )
      self.assertIsInstance(check_fn, test_utils.PyObjectIsEqualCheck)

  @parameterized.parameters(
      (arolla_types.int32(1), arolla_types.int32(1)),
      (arolla_types.float32(0.5), arolla_types.float32(0.375)),
      (
          arolla_types.array_float32([None, 0.25, NAN]),
          arolla_types.array_float32([None, 0.375, NAN]),
      ),
      (
          arolla_types.Dict({'a': 0.25, 'b': NAN, 'c': None}),
          arolla_types.Dict({'a': 0.25, 'b': NAN, 'c': None}),
      ),
  )
  def test_assert_qvalue_allclose_success(self, lhs, rhs):
    test_utils.assert_qvalue_allclose(lhs, rhs, rtol=0.1, atol=0.1)

  @parameterized.parameters(
      (arolla_types.int32(1), arolla_types.int32(2)),
      (arolla_types.float32(0.5), arolla_types.float32(0.25)),
      (arolla_types.array_float32([None]), arolla_types.array_float32([])),
      (arolla_types.Dict({'a': 1}), arolla_types.Dict({'a': 2})),
      (arolla_types.Dict({'a': 1.0, 'b': 2.0}), arolla_types.Dict({'a': 1.0})),
      (arolla_types.Dict({'a': 1.0, 'b': None}), arolla_types.Dict({'a': 1.0})),
      (arolla_types.Dict({'a': 1, 'b': 2}), arolla_types.array_int32([1, 2])),
  )
  def test_assert_qvalue_allclose_assertion_error(self, lhs, rhs):
    with self.assertRaises(AssertionError):
      test_utils.assert_qvalue_allclose(lhs, rhs, rtol=0.1, atol=0.1)

  @parameterized.parameters(
      (None, None),
      (1, 1),
      (0.5, 0.5),
  )
  def test_assert_qvalue_allclose_type_error(self, lhs, rhs):
    with self.assertRaises(TypeError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_allclose(lhs, rhs, rtol=0.1, atol=0.1)

  def test_assert_qvalue_allclose_message(self):
    message = None
    try:
      test_utils.assert_qvalue_allclose(
          arolla_types.array(range(100)), arolla_types.array(range(1, 101))
      )
    except AssertionError as ex:
      message = ex.args[0]
    self.assertEqual(
        message.split('\n'),
        [
            (
                'QValues not close: array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ...], '
                'size=100) != array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ...], '
                'size=100)'
            ),
            '',
            '  item[0]: not equal: actual=0, expected=1',
            '  item[1]: not equal: actual=1, expected=2',
            '  item[2]: not equal: actual=2, expected=3',
            '  item[3]: not equal: actual=3, expected=4',
            '  item[4]: not equal: actual=4, expected=5',
            '  item[5]: not equal: actual=5, expected=6',
            '  item[6]: not equal: actual=6, expected=7',
            '  item[7]: not equal: actual=7, expected=8',
            '  item[8]: not equal: actual=8, expected=9',
            '  item[9]: not equal: actual=9, expected=10',
            '  ...',
        ],
    )

  @parameterized.parameters(
      arolla_types.int32,
      arolla_types.tuple_(),
  )
  def test_assert_qvalue_allclose_message_type_error(self, arg):
    with self.assertRaises(TypeError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_allclose(arg, arg)

  @parameterized.parameters(
      (arolla_types.INT32, arolla_types.INT32),
      (arolla_types.int32(1), arolla_types.int32(1)),
      (arolla_types.optional_float32(0.5), arolla_types.optional_float32(0.5)),
      (
          arolla_types.array_float32([None, 0.25, NAN]),
          arolla_types.array_float32([None, 0.25, NAN]),
      ),
      (
          arolla_types.Dict({'a': 1, 'b': 2}),
          arolla_types.Dict({'a': 1, 'b': 2}),
      ),
  )
  def test_assert_qvalue_allequal_success(self, lhs, rhs):
    test_utils.assert_qvalue_allequal(lhs, rhs)

  @parameterized.parameters(
      (arolla_types.INT32, arolla_types.INT64),
      (arolla_types.int32(1), arolla_types.int32(2)),
      (arolla_types.float32(0.5), arolla_types.float64(0.5)),
      (arolla_types.array_float32([None]), arolla_types.array_float32([])),
      (arolla_types.Dict({'a': 1, 'b': 2}), arolla_types.Dict({'a': 1})),
      (arolla_types.Dict({'a': 1, 'b': None}), arolla_types.Dict({'a': 1})),
      (arolla_types.Dict({'a': 1, 'b': 2}), arolla_types.array_int32([1, 2])),
  )
  def test_assert_qvalue_allequal_assertion_error(self, lhs, rhs):
    with self.assertRaises(AssertionError):
      test_utils.assert_qvalue_allequal(lhs, rhs)

  @parameterized.parameters(
      (None, None),
      (1, 1),
      (0.5, 0.5),
  )
  def test_assert_qvalue_allequal_type_error(self, lhs, rhs):
    with self.assertRaises(TypeError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_allequal(lhs, rhs)

  def test_assert_qvalue_allequal_message(self):
    message = None
    try:
      test_utils.assert_qvalue_allequal(
          arolla_types.array(range(100)), arolla_types.array(range(1, 101))
      )
    except AssertionError as ex:
      message = ex.args[0]
    self.assertEqual(
        message.split('\n'),
        [
            (
                'QValues not equal: array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, ...], '
                'size=100) != array([1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ...], '
                'size=100)'
            ),
            '',
            '  item[0]: not equal: actual=0, expected=1',
            '  item[1]: not equal: actual=1, expected=2',
            '  item[2]: not equal: actual=2, expected=3',
            '  item[3]: not equal: actual=3, expected=4',
            '  item[4]: not equal: actual=4, expected=5',
            '  item[5]: not equal: actual=5, expected=6',
            '  item[6]: not equal: actual=6, expected=7',
            '  item[7]: not equal: actual=7, expected=8',
            '  item[8]: not equal: actual=8, expected=9',
            '  item[9]: not equal: actual=9, expected=10',
            '  ...',
        ],
    )

  @parameterized.parameters(
      M.math.add,
      arolla_types.tuple_(),
  )
  def test_assert_qvalue_allequal_message_type_error(self, arg):
    with self.assertRaises(TypeError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_allequal(arg, arg)

  @parameterized.parameters(
      (arolla_types.INT32, arolla_types.INT32),
      (arolla_types.int32(1), arolla_types.int32(1)),
      (arolla_types.optional_float32(0.5), arolla_types.optional_float32(0.5)),
      (
          arolla_types.array_float32([None, 0.25, NAN]),
          arolla_types.array_float32([None, 0.25, NAN]),
      ),
  )
  def test_assert_qvalue_equal_by_fingerprint_success(self, lhs, rhs):
    test_utils.assert_qvalue_equal_by_fingerprint(lhs, rhs)

  @parameterized.parameters(
      (arolla_types.INT32, arolla_types.INT64),
      (arolla_types.int32(1), arolla_types.int32(2)),
      (arolla_types.float32(0.5), arolla_types.float64(0.5)),
      (arolla_types.array_float32([None]), arolla_types.array_float32([])),
  )
  def test_assert_qvalue_equal_by_fingerprint_assertion_error(self, lhs, rhs):
    with self.assertRaises(AssertionError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_equal_by_fingerprint(lhs, rhs)

  @parameterized.parameters(
      (arolla_types.int32(1), arolla_types.int32(2)),
      (arolla_types.float32(0.5), arolla_types.float32(0.25)),
      (arolla_types.array_float32([None]), arolla_types.array_float32([])),
  )
  def test_assert_qvalue_equal_by_fingerprint_error(self, lhs, rhs):
    with self.assertRaises(AssertionError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_equal_by_fingerprint(lhs, rhs)

  @parameterized.parameters((M.math.add, 1), (1, M.math.add), (1, 2))
  def test_assert_qvalue_equal_by_fingerprint_type_error(self, lhs, rhs):
    with self.assertRaises(TypeError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_equal_by_fingerprint(lhs, rhs)

  def test_assert_qvalue_equal_by_fingerprint_error_message(self):
    try:
      with mock.patch.object(
          arolla_abc.Unspecified, '__repr__', lambda x: 'repr:unspecified'
      ):
        with mock.patch.object(
            arolla_abc.Unspecified, '__str__', lambda x: 'str:unspecified'
        ):
          test_utils.assert_qvalue_equal_by_fingerprint(
              arolla_types.int32(42), arolla_abc.unspecified()
          )
    except AssertionError as ex:
      message = ex.args[0]

    self.assertIn('QValues not equal: 42 != repr:unspecified', message)

  @parameterized.parameters((L.x), (L.x + L.y), (1 + L.x * L.y))
  def test_assert_expr_equal_by_fingerprint_success(self, expr):
    test_utils.assert_expr_equal_by_fingerprint(expr, expr)

  @parameterized.parameters(
      (arolla_types.as_expr(0), arolla_types.as_expr(1)),
      (arolla_types.as_expr(0), arolla_types.as_expr(0.0)),
      (arolla_types.as_expr(arolla_types.array_float32([None])), L.x),
  )
  def test_assert_expr_equal_by_fingerprint_assertion_error(self, lhs, rhs):
    with self.assertRaises(AssertionError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_expr_equal_by_fingerprint(lhs, rhs)

  def test_assert_expr_equal_by_fingerprint_error_message(self):
    lhs = M.annotation.name(L.x, 'foo') + 1
    rhs = M.annotation.name(L.x, 'foo') + L.y
    message = None
    try:
      test_utils.assert_expr_equal_by_fingerprint(lhs, rhs)
    except AssertionError as ex:
      message = ex.args[0]

    self.assertEqual(
        message.split('\n'),
        [
            'Exprs not equal by fingerprint:',
            (
                f'  actual_fingerprint={lhs.fingerprint},'
                + f' expected_fingerprint={rhs.fingerprint}'
            ),
            '  actual:',
            '    foo = L.x',
            '    M.math.add(foo, 1)',
            '  expected:',
            '    foo = L.x',
            '    M.math.add(foo, L.y)',
        ],
    )


if __name__ == '__main__':
  absltest.main()
