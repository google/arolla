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

"""Tests for M.array.at."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_qtype_signatures():
  for arg_1 in pointwise_test_utils.lift_qtypes(
      *arolla.types.SCALAR_QTYPES,
      mutators=pointwise_test_utils.ARRAY_QTYPE_MUTATORS,
  ):
    for arg_2 in pointwise_test_utils.lift_qtypes(
        *arolla.types.INTEGRAL_QTYPES
    ):
      yield (
          arg_1,
          arg_2,
          arolla.types.broadcast_qtype(
              [arolla.OPTIONAL_UNIT, arg_2], arg_1.value_qtype
          ),
      )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())

TEST_DATA = (
    [True, None, True],
    [True, None, False],
    [2, None, 57],
    ['two', None, 'fifty seven'],
    [b'two', None, b'fifty seven'],
)

ARRAY_FACTORIES = (
    arolla.types.dense_array_unit,
    arolla.types.dense_array_boolean,
    arolla.types.dense_array_bytes,
    arolla.types.dense_array_text,
    arolla.types.dense_array_int32,
    arolla.types.dense_array_int64,
    arolla.types.dense_array_uint64,
    arolla.types.dense_array_float32,
    arolla.types.dense_array_float64,
    arolla.types.dense_array_weak_float,
    arolla.types.array_unit,
    arolla.types.array_boolean,
    arolla.types.array_bytes,
    arolla.types.array_text,
    arolla.types.array_int32,
    arolla.types.array_int64,
    arolla.types.array_uint64,
    arolla.types.array_float32,
    arolla.types.array_float64,
    arolla.types.array_weak_float,
)


def gen_test_data(test_arrays, *qtype_sets):
  """Generates (array, id(s), expected_result(s)) tuples."""
  untested_signatures = set(qtype_sets)
  for array in test_arrays:
    for factory in ARRAY_FACTORIES:
      try:
        array_qvalue = factory(array)
      except (
          ValueError,
          TypeError,
      ):  # `factory` does not support element type of `array`.
        continue
      matching_signatures = set(
          ts for ts in qtype_sets if ts[0] == array_qvalue.qtype
      )
      untested_signatures -= matching_signatures
      test_data = tuple((i, array[i]) for i in range(len(array))) + (
          (None, None),
      )
      qtypes = (ts[1:] for ts in matching_signatures)
      for case in pointwise_test_utils.gen_cases(test_data, *qtypes):
        yield (array_qvalue, *case)
  if untested_signatures:
    raise RuntimeError(
        'No test data for signatures: '
        + ', '.join(map(str, untested_signatures))
    )


def gen_error_test_data(test_arrays):
  """Generates (array, id(s), expected_error_message(s)) tuples."""
  for array in test_arrays:
    for factory in ARRAY_FACTORIES:
      try:
        array_qvalue = factory(array)
      except (
          ValueError,
          TypeError,
      ):  # `factory` does not support element type of `array`.
        continue
      yield (
          array_qvalue,
          arolla.int64(-1),
          f'array index -1 out of range [0, {len(array)})',
      )
      yield (
          array_qvalue,
          arolla.int64(len(array)),
          f'array index {len(array)} out of range [0, {len(array)})',
      )
      for ids_array_factory in (arolla.dense_array_int64, arolla.array_int64):
        # Always reporting the first error.
        yield (
            array_qvalue,
            ids_array_factory([0, -1, len(array)]),
            f'array index -1 out of range [0, {len(array)})',
        )


class ArrayAtTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(pointwise_test_utils.detect_qtype_signatures(M.array.at)),
    )

  @parameterized.parameters(gen_test_data(TEST_DATA, *QTYPE_SIGNATURES))
  def test_values(self, array, index, expected):
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.at(array, index)), expected
    )

  @parameterized.parameters(gen_error_test_data(TEST_DATA))
  def test_generic_errors(self, array, index, expected_error):
    # Hack to check that ANY error is raised for the error test data for all
    # backends. The error messages might differ between backends, so we avoid
    # checking this specifically.
    with self.assertRaises(Exception):
      self.eval(M.array.at(array, index))

  @parameterized.parameters(gen_error_test_data(TEST_DATA))
  def test_specific_error_messages(self, array, index, expected_error):
    # Error checks specific messages. Most suitable for the RLv2 backend.
    with self.assertRaises(ValueError) as catched:
      self.eval(M.array.at(array, index))
    self.assertIn(expected_error, catched.exception.args[0])


if __name__ == '__main__':
  absltest.main()
