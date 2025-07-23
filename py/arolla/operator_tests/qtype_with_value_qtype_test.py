# Copyright 2025 Google LLC
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

"""Tests for M.qtype.with_value_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

NAN = float("nan")

M = arolla.M


def gen_test_data():
  """Yields test data for qtype.with_value_qtype operator.

  Yields: (arg_1, arg_2, result)
  """
  for arg_2 in arolla.types.SCALAR_QTYPES:
    yield from (
        (arolla.types.SCALAR_SHAPE, arg_2, arg_2),
        (
            arolla.types.OPTIONAL_SCALAR_SHAPE,
            arg_2,
            arolla.types.make_optional_qtype(arg_2),
        ),
        (
            arolla.types.DENSE_ARRAY_SHAPE,
            arg_2,
            arolla.types.make_dense_array_qtype(arg_2),
        ),
        (arolla.types.ARRAY_SHAPE, arg_2, arolla.types.make_array_qtype(arg_2)),
    )
  yield from (
      (arolla.FLOAT32, arolla.OPTIONAL_INT32, arolla.NOTHING),
      (arolla.OPTIONAL_FLOAT32, arolla.OPTIONAL_INT32, arolla.NOTHING),
      (arolla.DENSE_ARRAY_FLOAT32, arolla.OPTIONAL_INT32, arolla.NOTHING),
      (arolla.ARRAY_FLOAT32, arolla.OPTIONAL_INT32, arolla.NOTHING),
  )


TEST_DATA = tuple(gen_test_data())

# QType signatures: tuple(
#     (arg_1_qtype, arg_2_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.QTYPE, arolla.QTYPE),)


class QTypeWithValueQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.qtype.with_value_qtype, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg_1, arg_2, expected_value):
    actual_value = arolla.eval(M.qtype.with_value_qtype(arg_1, arg_2))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
