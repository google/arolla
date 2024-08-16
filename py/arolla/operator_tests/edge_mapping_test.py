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

"""Tests for edge.mapping operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M


def gen_test_cases():
  """Yields test cases for edge.mapping operator.

  Yields: (edge_qvalue, result_qvalue)
  """
  mapping_values = (
      [],
      [None],
      [4, 3, 1, None, 5, 2, None],
  )
  for mapping in mapping_values:
    for _, array_fn in utils.ARRAY_FACTORIES:
      array = array_fn(mapping, arolla.INT64)
      yield M.edge.from_mapping(array, parent_size=7), array
      yield M.edge.to_scalar(array), array_fn([0] * len(mapping), arolla.INT64)
  split_points_data = (([0], []), ([0, 2, 2, 5], [0, 0, 2, 2, 2]))
  for split_points, mapping in split_points_data:
    for _, array_fn in utils.ARRAY_FACTORIES:
      yield M.edge.from_split_points(
          array_fn(split_points, arolla.INT64)
      ), array_fn(mapping, arolla.INT64)


TEST_CASES = tuple(gen_test_cases())

# QType signatures for edge.from_mapping.
#
#   ((edge_qtype, result_qtype), ...)
QTYPE_SIGNATURES = (
    (arolla.eval(M.qtype.qtype_of(edge)), arolla.eval(M.qtype.qtype_of(array)))
    for edge, array in TEST_CASES
)


class EdgeMappingTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(M.edge.mapping, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_CASES)
  def test_simple(self, edge, mapping):
    result = self.eval(M.edge.mapping(edge))
    arolla.testing.assert_qvalue_allequal(result, mapping)


if __name__ == '__main__':
  absltest.main()
