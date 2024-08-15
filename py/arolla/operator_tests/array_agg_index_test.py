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

"""Tests for array.agg_index."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M


def gen_qtype_signatures():
  yield from (
      (array_type, arolla.ARRAY_EDGE, arolla.types.ARRAY_INT64)
      for array_type in arolla.types.ARRAY_QTYPES
  )
  yield from (
      (array_type, arolla.ARRAY_TO_SCALAR_EDGE, arolla.types.ARRAY_INT64)
      for array_type in arolla.types.ARRAY_QTYPES
  )
  yield from (
      (
          dense_array_type,
          arolla.types.DENSE_ARRAY_EDGE,
          arolla.types.DENSE_ARRAY_INT64,
      )
      for dense_array_type in arolla.types.DENSE_ARRAY_QTYPES
  )
  yield from (
      (
          dense_array_type,
          arolla.types.DENSE_ARRAY_TO_SCALAR_EDGE,
          arolla.types.DENSE_ARRAY_INT64,
      )
      for dense_array_type in arolla.types.DENSE_ARRAY_QTYPES
  )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class ArrayAggIndexTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.agg_index, QTYPE_SIGNATURES
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_agg_index(self, array_factory):
    values = array_factory([20, None, 20, 20, 20], arolla.types.INT32)
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 2])))
    expected = array_factory([0, None, 2, 0, 1], arolla.types.INT64)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.agg_index(values, over=edge)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_array_agg_index_scalar_edge(self, array_factory):
    values = array_factory([20, None, 20, 20, 20], arolla.INT32)
    edge = arolla.eval(M.edge.to_scalar(values))
    expected = array_factory([0, None, 2, 3, 4], arolla.types.INT64)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.agg_index(values, over=edge)), expected
    )


if __name__ == "__main__":
  absltest.main()
