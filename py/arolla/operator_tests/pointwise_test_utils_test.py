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

"""Tests for pointwise operator utils."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils


IntOnly = pointwise_test_utils.IntOnly

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


class PointwiseTestUtils(parameterized.TestCase):

  @parameterized.parameters(
      None,
      True,
      b'bar',
      'foo',
      1,
      2.5,
      IntOnly(3),
      arolla.unit(),
      arolla.bytes(b'foo'),
      arolla.text('bar'),
      arolla.int32(4),
      arolla.float32(5.0),
      arolla.OPTIONAL_UNIT,
  )
  def test_validate_item_ok(self, value):
    try:
      pointwise_test_utils._validate_item(value)
    except ValueError:
      self.fail('_validate_item() raised an exception!')

  @parameterized.parameters(
      [tuple()],
      arolla.missing_unit(),
  )
  def test_validate_item_value_error(self, value):
    with self.assertRaises(ValueError):
      pointwise_test_utils._validate_item(value)

  def test_validate_row(self):
    try:
      pointwise_test_utils._validate_row((1, 2))
    except ValueError:
      self.fail('_validate_row() raised an exception!')
    with self.assertRaises(ValueError):
      pointwise_test_utils._validate_row('foo')

  def test_validate_test_data(self):
    try:
      pointwise_test_utils._validate_test_data((
          (1, 2),
          (3, 4),
      ))
    except ValueError:
      self.fail('_validate_test_data() raised an exception!')
    with self.assertRaises(ValueError):
      pointwise_test_utils._validate_test_data(('foo',))

  def test_group_by_columns(self):
    test_data = (
        ('a', 5, 10),
        ('b', 7, 10),
        ('a', 7, 10),
    )
    self.assertEqual(
        pointwise_test_utils._group_by_columns(test_data),
        (
            (
                ('a', 5, 10),
                ('b', 7, 10),
                ('a', 7, 10),
            ),
        ),
    )
    self.assertEqual(
        pointwise_test_utils._group_by_columns(test_data, 0),
        (
            (
                ('a', 5, 10),
                ('a', 7, 10),
            ),
            (('b', 7, 10),),
        ),
    )
    self.assertEqual(
        pointwise_test_utils._group_by_columns(test_data, 1, 2),
        (
            (('a', 5, 10),),
            (
                ('b', 7, 10),
                ('a', 7, 10),
            ),
        ),
    )
    self.assertEqual(
        pointwise_test_utils._group_by_columns(test_data, 0, 1, 2),
        (
            (('a', 5, 10),),
            (('b', 7, 10),),
            (('a', 7, 10),),
        ),
    )

  def test_group_by_columns_regression_1(self):
    with self.subTest('SignOfZero'):
      test_data = (
          (-0.0, 'a'),
          (0, 'b'),
          (0.0, 'c'),
      )
      self.assertEqual(
          pointwise_test_utils._group_by_columns(test_data, 0),
          (
              ((-0.0, 'a'),),
              ((0, 'b'),),
              ((0.0, 'c'),),
          ),
      )
    with self.subTest('NaN'):
      test_data = (
          (float('NaN'), 'a'),
          (float('NaN'), 'b'),
      )
      result = pointwise_test_utils._group_by_columns(test_data, 0)
      # The expected result is a single group with two rows:
      # (
      #   (
      #       (NaN, 'a'),
      #       (NaN, 'b'),
      #   ),
      # )
      # But the direct comparison with assertEqual doesn't work
      # because of NaN != NaN.
      self.assertLen(result, 1)
      self.assertLen(result[0], 2)

  def test_group_by_columns_regression_2(self):
    # Similar to the regression_1, but with qvalues.
    with self.subTest('SignOfZero'):
      test_data = (
          (arolla.float64(-0.0), 'a'),
          (arolla.float64(0.0), 'c'),
      )
      self.assertEqual(
          pointwise_test_utils._group_by_columns(test_data, 0),
          (
              ((arolla.float64(-0.0), 'a'),),
              ((arolla.float64(0.0), 'c'),),
          ),
      )
    with self.subTest('NaN'):
      test_data = (
          (arolla.float64(float('NaN')), 'a'),
          (arolla.float64(float('NaN')), 'b'),
      )
      result = pointwise_test_utils._group_by_columns(test_data, 0)
      self.assertLen(result, 1)
      self.assertLen(result[0], 2)

  def test_is_qtype(self):
    self.assertTrue(pointwise_test_utils._is_qtype(arolla.UNIT))
    self.assertFalse(pointwise_test_utils._is_qtype(None))
    self.assertFalse(pointwise_test_utils._is_qtype(arolla.unit()))

  def test_is_unit(self):
    self.assertFalse(pointwise_test_utils._is_unit(None))
    self.assertFalse(pointwise_test_utils._is_unit(False))
    self.assertTrue(pointwise_test_utils._is_unit(True))
    self.assertFalse(pointwise_test_utils._is_unit(1.5))
    self.assertTrue(pointwise_test_utils._is_unit(arolla.unit()))
    self.assertFalse(pointwise_test_utils._is_unit(arolla.boolean(True)))

  def test_is_boolean(self):
    self.assertFalse(pointwise_test_utils._is_boolean(None))
    self.assertTrue(pointwise_test_utils._is_boolean(False))
    self.assertTrue(pointwise_test_utils._is_boolean(True))
    self.assertFalse(pointwise_test_utils._is_boolean(1))
    self.assertFalse(pointwise_test_utils._is_boolean(1.5))
    self.assertFalse(pointwise_test_utils._is_boolean(IntOnly(1)))
    self.assertTrue(pointwise_test_utils._is_boolean(arolla.boolean(True)))
    self.assertFalse(pointwise_test_utils._is_boolean(arolla.unit()))

  def test_is_bytes(self):
    self.assertFalse(pointwise_test_utils._is_bytes(None))
    self.assertTrue(pointwise_test_utils._is_bytes(b'bar'))
    self.assertFalse(pointwise_test_utils._is_bytes('foo'))
    self.assertTrue(pointwise_test_utils._is_bytes(arolla.bytes(b'foo')))
    self.assertFalse(pointwise_test_utils._is_bytes(arolla.text('foo')))

  def test_is_text(self):
    self.assertFalse(pointwise_test_utils._is_text(None))
    self.assertFalse(pointwise_test_utils._is_text(b'bar'))
    self.assertTrue(pointwise_test_utils._is_text('foo'))
    self.assertTrue(pointwise_test_utils._is_text(arolla.text('foo')))
    self.assertFalse(pointwise_test_utils._is_text(arolla.bytes(b'foo')))

  def test_is_int32(self):
    self.assertFalse(pointwise_test_utils._is_int32(None))
    self.assertFalse(pointwise_test_utils._is_int32(False))
    self.assertTrue(pointwise_test_utils._is_int32(10))
    self.assertTrue(pointwise_test_utils._is_int32(2**31 - 1))
    self.assertTrue(pointwise_test_utils._is_int32(-(2**31)))
    self.assertFalse(pointwise_test_utils._is_int32(2**31))
    self.assertFalse(pointwise_test_utils._is_int32(-(2**31) - 1))
    self.assertFalse(pointwise_test_utils._is_int32('foo'))
    self.assertTrue(pointwise_test_utils._is_int32(IntOnly(1)))
    self.assertTrue(pointwise_test_utils._is_int32(arolla.int32(1)))
    self.assertFalse(pointwise_test_utils._is_int32(arolla.int64(1)))

  def test_is_int64(self):
    self.assertFalse(pointwise_test_utils._is_int64(None))
    self.assertFalse(pointwise_test_utils._is_int64(False))
    self.assertTrue(pointwise_test_utils._is_int64(10))
    self.assertTrue(pointwise_test_utils._is_int64(2**63 - 1))
    self.assertTrue(pointwise_test_utils._is_int64(-(2**63)))
    self.assertFalse(pointwise_test_utils._is_int64(2**63))
    self.assertFalse(pointwise_test_utils._is_int64(-(2**63) - 1))
    self.assertFalse(pointwise_test_utils._is_int64('foo'))
    self.assertTrue(pointwise_test_utils._is_int64(IntOnly(1)))
    self.assertFalse(pointwise_test_utils._is_int64(arolla.int32(1)))
    self.assertTrue(pointwise_test_utils._is_int64(arolla.int64(1)))

  def test_is_float32(self):
    self.assertFalse(pointwise_test_utils._is_float32(None))
    self.assertFalse(pointwise_test_utils._is_float32(False))
    self.assertTrue(pointwise_test_utils._is_float32(10.0))
    self.assertTrue(pointwise_test_utils._is_float32(1e30))
    self.assertTrue(pointwise_test_utils._is_float32(float('nan')))
    self.assertTrue(pointwise_test_utils._is_float32(float('inf')))
    self.assertFalse(pointwise_test_utils._is_float32(1e100))
    self.assertTrue(pointwise_test_utils._is_float32(10))
    self.assertFalse(pointwise_test_utils._is_float32(10**100))
    self.assertFalse(pointwise_test_utils._is_float32(10**1000))
    self.assertFalse(pointwise_test_utils._is_float32(IntOnly(1)))
    self.assertTrue(pointwise_test_utils._is_float32(arolla.float32(1)))
    self.assertFalse(pointwise_test_utils._is_float32(arolla.float64(1)))

  def test_is_float64(self):
    self.assertFalse(pointwise_test_utils._is_float64(None))
    self.assertFalse(pointwise_test_utils._is_float64(False))
    self.assertTrue(pointwise_test_utils._is_float64(10.0))
    self.assertTrue(pointwise_test_utils._is_float64(1e100))
    self.assertTrue(pointwise_test_utils._is_float64(float('nan')))
    self.assertTrue(pointwise_test_utils._is_float64(float('inf')))
    self.assertTrue(pointwise_test_utils._is_float64(10))
    self.assertFalse(pointwise_test_utils._is_float64(10**1000))
    self.assertFalse(pointwise_test_utils._is_float64(IntOnly(1)))
    self.assertFalse(pointwise_test_utils._is_float64(arolla.float32(1)))
    self.assertTrue(pointwise_test_utils._is_float64(arolla.float64(1)))

  def test_is_weak_float(self):
    self.assertFalse(pointwise_test_utils._is_weak_float(None))
    self.assertFalse(pointwise_test_utils._is_weak_float(False))
    self.assertTrue(pointwise_test_utils._is_weak_float(10.0))
    self.assertTrue(pointwise_test_utils._is_weak_float(1e100))
    self.assertTrue(pointwise_test_utils._is_weak_float(float('nan')))
    self.assertTrue(pointwise_test_utils._is_weak_float(float('inf')))
    self.assertTrue(pointwise_test_utils._is_weak_float(10))
    self.assertFalse(pointwise_test_utils._is_weak_float(10**1000))
    self.assertFalse(pointwise_test_utils._is_weak_float(IntOnly(1)))
    self.assertTrue(pointwise_test_utils._is_weak_float(arolla.weak_float(1)))
    self.assertFalse(pointwise_test_utils._is_weak_float(arolla.float32(1)))
    self.assertFalse(pointwise_test_utils._is_weak_float(arolla.float64(1)))

  def test_gen_cases_qtype(self):
    test_data = (
        (arolla.INT32, None),
        (arolla.INT64, True),
    )
    actual_cases = arolla.tuple(
        *pointwise_test_utils.gen_cases(
            test_data,
            (arolla.QTYPE, arolla.UNIT),
            (arolla.QTYPE, arolla.OPTIONAL_UNIT),
            (arolla.QTYPE, arolla.DENSE_ARRAY_UNIT),
        )
    )
    self.assertEqual(
        actual_cases.fingerprint,
        arolla.tuple(
            (arolla.INT64, arolla.unit()),
            (arolla.INT32, arolla.optional_unit(None)),
            (arolla.INT64, arolla.optional_unit(True)),
            (arolla.INT32, arolla.dense_array_unit([None])),
            (arolla.INT64, arolla.dense_array_unit([True])),
        ).fingerprint,
    )

  def test_gen_cases_bool_like(self):
    test_data = (
        (None, None),
        (False, True),
    )
    actual_cases = arolla.tuple(
        *pointwise_test_utils.gen_cases(
            test_data,
            (arolla.BOOLEAN, arolla.UNIT),
            (arolla.OPTIONAL_BOOLEAN, arolla.OPTIONAL_UNIT),
            (arolla.DENSE_ARRAY_BOOLEAN, arolla.DENSE_ARRAY_UNIT),
        )
    )
    self.assertEqual(
        actual_cases.fingerprint,
        arolla.tuple(
            (arolla.boolean(False), arolla.unit()),
            (arolla.optional_boolean(None), arolla.optional_unit(None)),
            (arolla.optional_boolean(False), arolla.optional_unit(True)),
            (
                arolla.dense_array([None, False]),
                arolla.dense_array_unit([None, True]),
            ),
        ).fingerprint,
    )

  def test_gen_cases_str_like(self):
    test_data = (
        (None, None),
        (b'foo', 'bar'),
    )
    actual_cases = arolla.tuple(
        *pointwise_test_utils.gen_cases(
            test_data,
            (arolla.BYTES, arolla.TEXT),
            (arolla.OPTIONAL_BYTES, arolla.OPTIONAL_TEXT),
            (arolla.DENSE_ARRAY_BYTES, arolla.DENSE_ARRAY_TEXT),
        )
    )
    self.assertEqual(
        actual_cases.fingerprint,
        arolla.tuple(
            (arolla.bytes(b'foo'), arolla.text('bar')),
            (arolla.optional_bytes(None), arolla.optional_text(None)),
            (arolla.optional_bytes(b'foo'), arolla.optional_text('bar')),
            (
                arolla.dense_array([None, b'foo']),
                arolla.dense_array([None, 'bar']),
            ),
        ).fingerprint,
    )

  def test_gen_cases_numeric_like(self):
    test_data = (
        (None, None, None, None),
        (3, 4, 5.5, 6.5),
    )
    actual_cases = arolla.tuple(
        *pointwise_test_utils.gen_cases(
            test_data,
            (arolla.INT32, arolla.INT64, arolla.FLOAT32, arolla.FLOAT64),
            (
                arolla.OPTIONAL_INT32,
                arolla.OPTIONAL_INT64,
                arolla.OPTIONAL_FLOAT32,
                arolla.OPTIONAL_FLOAT64,
            ),
            (
                arolla.DENSE_ARRAY_INT32,
                arolla.DENSE_ARRAY_INT64,
                arolla.DENSE_ARRAY_FLOAT32,
                arolla.DENSE_ARRAY_FLOAT64,
            ),
        )
    )
    self.assertEqual(
        actual_cases.fingerprint,
        arolla.tuple(
            (
                arolla.int32(3),
                arolla.int64(4),
                arolla.float32(5.5),
                arolla.float64(6.5),
            ),
            (
                arolla.optional_int32(None),
                arolla.optional_int64(None),
                arolla.optional_float32(None),
                arolla.optional_float64(None),
            ),
            (
                arolla.optional_int32(3),
                arolla.optional_int64(4),
                arolla.optional_float32(5.5),
                arolla.optional_float64(6.5),
            ),
            (
                arolla.dense_array_int32([None, 3]),
                arolla.dense_array_int64([None, 4]),
                arolla.dense_array_float32([None, 5.5]),
                arolla.dense_array_float64([None, 6.5]),
            ),
        ).fingerprint,
    )

  def test_gen_cases_int_only(self):
    test_data = ((IntOnly(0), -1, 0),)
    with self.subTest('to_int32'):
      actual_cases = list(
          pointwise_test_utils.gen_cases(
              test_data, (arolla.INT32, arolla.INT32, arolla.INT32)
          )
      )
      self.assertEqual(
          actual_cases, [(arolla.int32(0), arolla.int32(-1), arolla.int32(0))]
      )
    with self.subTest('to_float32'):
      with self.assertRaisesRegex(RuntimeError, 'No test data'):
        _ = list(
            pointwise_test_utils.gen_cases(
                test_data, (arolla.FLOAT32, arolla.FLOAT32, arolla.FLOAT32)
            )
        )

  def test_gen_cases_different_arity(self):
    test_data = (
        # Binary.
        (None, 2),
        (1, 2),
        # Ternary.
        (None, 2, 3),
        (1, 2, None),
    )
    actual_cases = arolla.tuple(
        *pointwise_test_utils.gen_cases(
            test_data,
            # Binary.
            (arolla.OPTIONAL_INT32, arolla.OPTIONAL_INT32),
            (arolla.DENSE_ARRAY_INT32, arolla.DENSE_ARRAY_INT32),
            # Ternary.
            (
                arolla.OPTIONAL_INT32,
                arolla.OPTIONAL_INT32,
                arolla.OPTIONAL_INT32,
            ),
            (
                arolla.DENSE_ARRAY_INT32,
                arolla.DENSE_ARRAY_INT32,
                arolla.DENSE_ARRAY_INT32,
            ),
        )
    )
    self.assertEqual(
        actual_cases.fingerprint,
        arolla.tuple(
            # Binary.
            (arolla.optional_int32(None), arolla.optional_int32(2)),
            (arolla.optional_int32(1), arolla.optional_int32(2)),
            (
                arolla.dense_array_int32([None, 1]),
                arolla.dense_array_int32([2, 2]),
            ),
            # Ternary.
            (
                arolla.optional_int32(None),
                arolla.optional_int32(2),
                arolla.optional_int32(3),
            ),
            (
                arolla.optional_int32(1),
                arolla.optional_int32(2),
                arolla.optional_int32(None),
            ),
            (
                arolla.dense_array_int32([None, 1]),
                arolla.dense_array_int32([2, 2]),
                arolla.dense_array_int32([3, None]),
            ),
        ).fingerprint,
    )

  def test_lift_qtypes_tuple(self):
    actual_qtype_sets = pointwise_test_utils.lift_qtypes(
        (None, arolla.UNIT, arolla.OPTIONAL_FLOAT32)
    )
    self.assertListEqual(
        list(actual_qtype_sets),
        [
            (None, arolla.UNIT, arolla.OPTIONAL_FLOAT32),
            (None, arolla.OPTIONAL_UNIT, arolla.OPTIONAL_FLOAT32),
            (None, arolla.DENSE_ARRAY_UNIT, arolla.DENSE_ARRAY_FLOAT32),
            (None, arolla.ARRAY_UNIT, arolla.ARRAY_FLOAT32),
        ],
    )

  def test_lift_qtypes(self):
    actual_qtype_sets = pointwise_test_utils.lift_qtypes(
        None,
        arolla.UNIT,
        arolla.OPTIONAL_FLOAT32,
        mutators=[arolla.make_optional_qtype, arolla.make_array_qtype],
    )
    self.assertListEqual(
        list(actual_qtype_sets),
        [
            None,
            arolla.OPTIONAL_UNIT,
            arolla.ARRAY_UNIT,
            arolla.OPTIONAL_FLOAT32,
            arolla.ARRAY_FLOAT32,
        ],
    )

  def test_lift_qtypes_returns_tuple(self):
    actual_qtype_sets = pointwise_test_utils.lift_qtypes(
        None, arolla.UNIT, (arolla.OPTIONAL_FLOAT32, arolla.INT32)
    )
    self.assertIsInstance(actual_qtype_sets, tuple)


if __name__ == '__main__':
  absltest.main()
