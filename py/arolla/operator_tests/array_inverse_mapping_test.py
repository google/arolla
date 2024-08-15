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

"""Tests for array_inverse_mapping."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils
# TODO: Test the set of available signatures(detect_type_signatures).

L = arolla.L
M = arolla.M


class ArrayInverseMappingTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_inverse_mapping(self, array_factory):
    permutation = array_factory([2, 0, 1, 1, 0])
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 2])))
    expected = array_factory([1, 2, 0, 1, 0])

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.inverse_mapping(permutation, over=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_inverse_mapping_default_edge(self, array_factory):
    permutation = array_factory([1, 3, 2, 0])
    expected = array_factory([3, 0, 2, 1])

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.inverse_mapping(permutation)), expected
    )

  def test_regression_crash_on_lowering_with_no_qtype(self):
    self.require_self_eval_is_called = False
    _ = arolla.abc.to_lowest(
        M.array.inverse_mapping(L.x)
    )  # expect no crash (b/240257524)


if __name__ == '__main__':
  absltest.main()
