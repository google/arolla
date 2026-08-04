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

"""Tests for M.qtype.get_value_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

L = arolla.L
M = arolla.M


def gen_test_data():
  """Yields test data for qtype.get_value_qtype operator.

  Yields: (arg, result)
  """
  qtypes = (
      arolla.types.SCALAR_QTYPES
      + arolla.types.OPTIONAL_QTYPES
      + arolla.types.DENSE_ARRAY_QTYPES
      + arolla.types.ARRAY_QTYPES
      + (
          arolla.NOTHING,
          arolla.make_tuple_qtype(),
          arolla.make_tuple_qtype(arolla.INT32),
      )
      + (
          arolla.eval(M.qtype.make_sequence_qtype(arolla.NOTHING)),
          arolla.eval(M.qtype.make_sequence_qtype(arolla.QTYPE)),
          arolla.eval(M.qtype.make_sequence_qtype(arolla.INT32)),
      )
  )
  for arg in qtypes:
    yield (arg, arg.value_qtype or arolla.NOTHING)


TEST_DATA = tuple(gen_test_data())

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.QTYPE),)


class QTypeGetValueQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.qtype.get_value_qtype, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.get_value_qtype(L.arg), arg=arg)
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_infer_attr(self, arg, expected_value):
    actual_value = M.qtype.get_value_qtype(arg).qvalue
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
