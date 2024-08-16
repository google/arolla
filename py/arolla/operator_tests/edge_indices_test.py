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

"""Tests for edge.indices."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import utils

M = arolla.M


class EdgeIndicesTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.edge.indices,
        [
            (arolla.ARRAY_EDGE, arolla.ARRAY_INT64),
            (arolla.ARRAY_TO_SCALAR_EDGE, arolla.ARRAY_INT64),
            (arolla.DENSE_ARRAY_EDGE, arolla.DENSE_ARRAY_INT64),
            (arolla.DENSE_ARRAY_TO_SCALAR_EDGE, arolla.DENSE_ARRAY_INT64),
        ],
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_edge_indices(self, array_factory):
    edge = M.edge.from_sizes(array_factory([3, 2]))
    expected = array_factory([0, 1, 2, 0, 1], arolla.types.INT64)

    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.indices(edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_edge_indices_scalar_edge(self, array_factory):
    edge = M.edge.to_scalar(array_factory([0, 0, 0, 0, 0]))
    expected = array_factory([0, 1, 2, 3, 4], arolla.types.INT64)

    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.indices(edge)), expected
    )


if __name__ == "__main__":
  absltest.main()
