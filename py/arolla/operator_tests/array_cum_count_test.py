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

"""Tests for array_cum_count."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M


@parameterized.named_parameters(*utils.ARRAY_FACTORIES)
class ArrayCumCountTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_array_cum_count(self, array_factory):
    values = array_factory([None, "hello", "hello", None, None, "world"])
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 2, 1])))
    expected = array_factory([None, 1, 2, None, None, 1], arolla.INT64)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.cum_count(values, edge)), expected
    )

  def test_array_cum_count_default_edge(self, array_factory):
    values = array_factory([None, "hello", "hello", None, None, "world"])
    expected = array_factory([None, 1, 2, None, None, 3], arolla.INT64)

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.cum_count(values)), expected
    )

  def test_array_cum_count_scalar(self, array_factory):
    values = array_factory([1, 2, 3, 10, None, 30], arolla.INT32)
    edge = arolla.eval(M.edge.to_scalar(values))

    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.cum_count(values, over=edge)),
        array_factory([1, 2, 3, 4, None, 5], arolla.INT64),
    )


if __name__ == "__main__":
  absltest.main()
