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

from absl.testing import absltest
from absl.testing import parameterized

from arolla.expr import expr as rl_expr
from arolla.testing import test_utils
from arolla.types import types as rl_types

L = rl_expr.LeafContainer()
M = rl_expr.OperatorsContainer()

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
      rl_types.int64(1), rl_types.float32(0.0), rl_types.float32(NAN)
  )
  def testQValueIsEqualByFingerprint_ok(self, x):
    check_fn = test_utils.QValueIsEqualByFingerprintCheck()
    self.assertIsNone(check_fn(x, x))

  @parameterized.parameters(
      (rl_types.int64(1), rl_types.int32(1)),
      (rl_types.float32(NAN), rl_types.float32(-NAN)),
      (rl_types.float32(0.0), rl_types.float32(-0.0)),
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
          rl_types.FLOAT32, rtol=0, atol=1
      )
      self.assertIsInstance(check_fn, test_utils.PyFloatIsCloseCheck)
      self.assertEqual(check_fn._rtol, 0)
      self.assertEqual(check_fn._atol, 1)
    with self.subTest('float32, rtol=dict, atol=dict'):
      check_fn = test_utils._make_allclose_check_predicate(
          rl_types.FLOAT32,
          rtol={
              rl_types.FLOAT32: 1,
              rl_types.FLOAT64: 2,
          },
          atol={
              rl_types.FLOAT32: 3,
              rl_types.FLOAT64: 4,
          },
      )
      self.assertIsInstance(check_fn, test_utils.PyFloatIsCloseCheck)
      self.assertEqual(check_fn._rtol, 1)
      self.assertEqual(check_fn._atol, 3)
    with self.subTest('int32'):
      check_fn = test_utils._make_allclose_check_predicate(
          rl_types.INT32, rtol=1, atol=1
      )
      self.assertIsInstance(check_fn, test_utils.PyObjectIsEqualCheck)
    with self.subTest('missing_rtol'):
      with self.assertRaises(KeyError):
        _ = test_utils._make_allclose_check_predicate(
            rl_types.FLOAT32, rtol={}, atol=1
        )
    with self.subTest('missing_atol'):
      with self.assertRaises(KeyError):
        _ = test_utils._make_allclose_check_predicate(
            rl_types.FLOAT32, rtol=1, atol={}
        )
      self.assertIsInstance(check_fn, test_utils.PyObjectIsEqualCheck)

  @parameterized.parameters(
      (rl_types.int32(1), rl_types.int32(1)),
      (rl_types.float32(0.5), rl_types.float32(0.375)),
      (
          rl_types.array_float32([None, 0.25, NAN]),
          rl_types.array_float32([None, 0.375, NAN]),
      ),
      (
          rl_types.Dict({'a': 0.25, 'b': NAN, 'c': None}),
          rl_types.Dict({'a': 0.25, 'b': NAN, 'c': None}),
      ),
  )
  def test_assert_qvalue_allclose_success(self, lhs, rhs):
    test_utils.assert_qvalue_allclose(lhs, rhs, rtol=0.1, atol=0.1)

  @parameterized.parameters(
      (rl_types.int32(1), rl_types.int32(2)),
      (rl_types.float32(0.5), rl_types.float32(0.25)),
      (rl_types.array_float32([None]), rl_types.array_float32([])),
      (rl_types.Dict({'a': 1}), rl_types.Dict({'a': 2})),
      (rl_types.Dict({'a': 1.0, 'b': 2.0}), rl_types.Dict({'a': 1.0})),
      (rl_types.Dict({'a': 1.0, 'b': None}), rl_types.Dict({'a': 1.0})),
      (rl_types.Dict({'a': 1, 'b': 2}), rl_types.array_int32([1, 2])),
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
          rl_types.array(range(100)), rl_types.array(range(1, 101))
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
      rl_types.int32,
      rl_types.tuple_(),
  )
  def test_assert_qvalue_allclose_message_type_error(self, arg):
    with self.assertRaises(TypeError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_allclose(arg, arg)

  @parameterized.parameters(
      (rl_types.INT32, rl_types.INT32),
      (rl_types.int32(1), rl_types.int32(1)),
      (rl_types.optional_float32(0.5), rl_types.optional_float32(0.5)),
      (
          rl_types.array_float32([None, 0.25, NAN]),
          rl_types.array_float32([None, 0.25, NAN]),
      ),
      (rl_types.Dict({'a': 1, 'b': 2}), rl_types.Dict({'a': 1, 'b': 2})),
  )
  def test_assert_qvalue_allequal_success(self, lhs, rhs):
    test_utils.assert_qvalue_allequal(lhs, rhs)

  @parameterized.parameters(
      (rl_types.INT32, rl_types.INT64),
      (rl_types.int32(1), rl_types.int32(2)),
      (rl_types.float32(0.5), rl_types.float64(0.5)),
      (rl_types.array_float32([None]), rl_types.array_float32([])),
      (rl_types.Dict({'a': 1, 'b': 2}), rl_types.Dict({'a': 1})),
      (rl_types.Dict({'a': 1, 'b': None}), rl_types.Dict({'a': 1})),
      (rl_types.Dict({'a': 1, 'b': 2}), rl_types.array_int32([1, 2])),
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
          rl_types.array(range(100)), rl_types.array(range(1, 101))
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
      rl_types.tuple_(),
  )
  def test_assert_qvalue_allequal_message_type_error(self, arg):
    with self.assertRaises(TypeError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_allequal(arg, arg)

  @parameterized.parameters(
      (rl_types.INT32, rl_types.INT32),
      (rl_types.int32(1), rl_types.int32(1)),
      (rl_types.optional_float32(0.5), rl_types.optional_float32(0.5)),
      (
          rl_types.array_float32([None, 0.25, NAN]),
          rl_types.array_float32([None, 0.25, NAN]),
      ),
  )
  def test_assert_qvalue_equal_by_fingerprint_success(self, lhs, rhs):
    test_utils.assert_qvalue_equal_by_fingerprint(lhs, rhs)

  @parameterized.parameters(
      (rl_types.INT32, rl_types.INT64),
      (rl_types.int32(1), rl_types.int32(2)),
      (rl_types.float32(0.5), rl_types.float64(0.5)),
      (rl_types.array_float32([None]), rl_types.array_float32([])),
  )
  def test_assert_qvalue_equal_by_fingerprint_assertion_error(self, lhs, rhs):
    with self.assertRaises(AssertionError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_equal_by_fingerprint(lhs, rhs)

  @parameterized.parameters(
      (rl_types.int32(1), rl_types.int32(2)),
      (rl_types.float32(0.5), rl_types.float32(0.25)),
      (rl_types.array_float32([None]), rl_types.array_float32([])),
  )
  def test_assert_qvalue_equal_by_fingerprint_error(self, lhs, rhs):
    with self.assertRaises(AssertionError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_equal_by_fingerprint(lhs, rhs)

  @parameterized.parameters((M.math.add, 1), (1, M.math.add), (1, 2))
  def test_assert_qvalue_equal_by_fingerprint_type_error(self, lhs, rhs):
    with self.assertRaises(TypeError):  # pylint: disable=g-error-prone-assert-raises
      test_utils.assert_qvalue_equal_by_fingerprint(lhs, rhs)

  @parameterized.parameters((L.x), (L.x + L.y), (1 + L.x * L.y))
  def test_assert_expr_equal_by_fingerprint_success(self, expr):
    test_utils.assert_expr_equal_by_fingerprint(expr, expr)

  @parameterized.parameters(
      (rl_types.as_expr(0), rl_types.as_expr(1)),
      (rl_types.as_expr(0), rl_types.as_expr(0.0)),
      (rl_types.as_expr(rl_types.array_float32([None])), L.x),
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
