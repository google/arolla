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

"""Tests for M.array.select."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_test_data():
  for array_fn in (arolla.array, arolla.dense_array):
    for tpe in arolla.types.NUMERIC_QTYPES + (arolla.types.UINT64,):
      yield (
          array_fn([1, 2, 3], tpe),
          array_fn(
              [arolla.present(), arolla.present(), arolla.present()],
              arolla.types.UNIT,
          ),
          array_fn([1, 2, 3], tpe),
      )
      yield (
          array_fn([1, None, 3], tpe),
          array_fn(
              [arolla.present(), arolla.present(), arolla.present()],
              arolla.types.UNIT,
          ),
          array_fn([1, None, 3], tpe),
      )
      yield (
          array_fn([1, 2, 3], tpe),
          array_fn(
              [arolla.missing(), arolla.present(), arolla.missing()],
              arolla.types.UNIT,
          ),
          array_fn([2], tpe),
      )
      yield (
          array_fn([None, 2, None], tpe),
          array_fn(
              [arolla.missing(), arolla.present(), arolla.missing()],
              arolla.types.UNIT,
          ),
          array_fn([2], tpe),
      )
      yield (
          array_fn([None, 2, 3], tpe),
          array_fn(
              [arolla.present(), arolla.missing(), arolla.missing()],
              arolla.types.UNIT,
          ),
          array_fn([None], tpe),
      )
      yield (
          array_fn([1, 2, 3], tpe),
          array_fn(
              [arolla.missing(), arolla.missing(), arolla.missing()],
              arolla.types.UNIT,
          ),
          array_fn([], tpe),
      )
    yield (
        array_fn([True, False, None, None], arolla.types.BOOLEAN),
        array_fn(
            [
                arolla.present(),
                arolla.missing(),
                arolla.present(),
                arolla.missing(),
            ],
            arolla.types.UNIT,
        ),
        array_fn([True, None], arolla.types.BOOLEAN),
    )
    yield (
        array_fn(['1', '2', None, None]),
        array_fn([
            arolla.present(),
            arolla.missing(),
            arolla.present(),
            arolla.missing(),
        ]),
        array_fn(['1', None]),
    )
    yield (
        array_fn([b'1', b'2', None, None], arolla.types.BYTES),
        array_fn(
            [
                arolla.present(),
                arolla.missing(),
                arolla.present(),
                arolla.missing(),
            ],
            arolla.types.UNIT,
        ),
        array_fn([b'1', None], arolla.types.BYTES),
    )
    yield (
        array_fn([None], arolla.types.UNIT),
        array_fn([arolla.present()], arolla.types.UNIT),
        array_fn([None], arolla.types.UNIT),
    )


def gen_qtype_signatures():
  """Yields qtype signatures for M.array.select."""
  for arg_1 in pointwise_test_utils.lift_qtypes(
      *arolla.types.SCALAR_QTYPES,
      mutators=pointwise_test_utils.ARRAY_QTYPE_MUTATORS,
  ):
    for arg_2 in pointwise_test_utils.lift_qtypes(arolla.UNIT):
      yield (arg_1, arg_2, arg_1)


TEST_CASES = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple((x.qtype, y.qtype, z.qtype) for x, y, z in TEST_CASES)


class ArraySelectTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.select, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_CASES)
  def test_select_op(self, arg, mask, expected):
    actual = self.eval(M.array.select(arg, mask))
    arolla.testing.assert_qvalue_allequal(actual, expected)

  def test_size_not_match(self):
    with self.assertRaisesRegex(ValueError, 'argument sizes mismatch'):
      self.eval(
          M.array.select(
              arolla.array([1, 2, 3], arolla.types.INT64),
              arolla.array([arolla.present()], arolla.types.UNIT),
          )
      )


if __name__ == '__main__':
  absltest.main()
