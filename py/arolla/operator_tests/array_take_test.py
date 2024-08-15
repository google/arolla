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

"""Tests for array.take."""
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M


@parameterized.named_parameters(*utils.ARRAY_FACTORIES)
class ArrayTakeTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_array_take_with_common_edge(self, array_factory):
    values = array_factory([1, None, 3, 4])
    offsets = array_factory([0, 1, 1, 0])
    edge = arolla.eval(M.edge.from_sizes(array_factory([2, 2])))
    expected = array_factory([1, None, 4, 3])

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.take(values, offsets, over=edge, ids_over=edge)),
        expected,
    )

  def test_array_take_with_two_edges(self, array_factory):
    values = array_factory([1, None, 3, 4, 5, 6])
    offsets = array_factory([1, 2, 2, 0])
    values_edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    offsets_edge = arolla.eval(M.edge.from_sizes(array_factory([2, 2])))
    expected = array_factory([None, 3, 6, 4])

    arolla.testing.assert_qvalue_allequal(
        self.eval(
            M.array.take(
                values, offsets, over=values_edge, ids_over=offsets_edge
            )
        ),
        expected,
    )

  def test_array_take_default_edge(self, array_factory):
    values = array_factory([1, 3, 2, 0])
    offsets = array_factory([0, 2, 2, 0])
    expected = array_factory([1, 2, 2, 1])

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.take(values, offsets)), expected
    )

  def test_array_take_default_edges_with_different_sizes(self, array_factory):
    values = array_factory([1, None, 3, 4])
    offsets = array_factory([2, 3, 1, 2, None, 0])
    expected = array_factory([3, 4, None, 3, None, 1])

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.take(values, offsets)), expected
    )

  def test_array_take_edges_to_scalar(self, array_factory):
    values = array_factory([1, None, 3, 4])
    offsets = array_factory([2, 3, 1, 2, None, 0])
    values_edge = arolla.eval(M.edge.to_scalar(values))
    offsets_edge = arolla.eval(M.edge.to_scalar(offsets))
    expected = array_factory([3, 4, None, 3, None, 1])

    arolla.testing.assert_qvalue_allequal(
        self.eval(
            M.array.take(
                values, offsets, over=values_edge, ids_over=offsets_edge
            )
        ),
        expected,
    )

  def test_array_take_with_only_one_edge(self, array_factory):
    self.require_self_eval_is_called = False
    values = array_factory([1, None, 3, 4, 5, 6])
    offsets = array_factory([1, 2, 2, 0])
    values_edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3])))
    with self.assertRaisesRegex(
        ValueError, r'Two edges must share the parent side'
    ):
      arolla.eval(M.array.take(values, offsets, over=values_edge))

  def test_array_take_with_incompatible_edge_pair(self, array_factory):
    self.require_self_eval_is_called = False
    values = array_factory([1, None, 3, 4, 5, 6])
    offsets = array_factory([1, 2, 2, 0])
    values_edge = arolla.eval(M.edge.from_sizes(array_factory([3, 1, 2])))
    offsets_edge = arolla.eval(M.edge.from_sizes(array_factory([2, 2])))
    with self.assertRaisesRegex(
        ValueError, re.escape(r'argument sizes mismatch: (3, 2)')
    ):
      arolla.eval(
          M.array.take(values, offsets, over=values_edge, ids_over=offsets_edge)
      )


if __name__ == '__main__':
  absltest.main()
