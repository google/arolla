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

"""Tests for M.qtype.is_edge_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_test_data():
  """Yields: (arg, result)."""
  edge_qtypes = (
      arolla.types.SCALAR_TO_SCALAR_EDGE,
      arolla.DENSE_ARRAY_EDGE,
      arolla.DENSE_ARRAY_TO_SCALAR_EDGE,
      arolla.ARRAY_EDGE,
      arolla.ARRAY_TO_SCALAR_EDGE,
  )
  non_edge_qtypes = (
      (
          arolla.NOTHING,
          arolla.make_tuple_qtype(),
          arolla.make_tuple_qtype(arolla.INT32),
      )
      + (*arolla.types.SCALAR_QTYPES, arolla.SCALAR_SHAPE)
      + (*arolla.types.OPTIONAL_QTYPES, arolla.OPTIONAL_SCALAR_SHAPE)
      + (*arolla.types.DENSE_ARRAY_QTYPES, arolla.DENSE_ARRAY_SHAPE)
      + (*arolla.types.ARRAY_QTYPES, arolla.ARRAY_SHAPE)
  )
  yield from ((x, True) for x in edge_qtypes)
  yield from ((x, None) for x in non_edge_qtypes)


# Test data: tuple(
#     (arg, result),
#     ...
# )
TEST_DATA = tuple(gen_test_data())

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = ((arolla.QTYPE, arolla.OPTIONAL_UNIT),)


class QTypeIsEdgeQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.qtype.is_edge_qtype, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, arg, expected_value):
    actual_value = arolla.eval(M.qtype.is_edge_qtype(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
