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

"""Tests for array_dense_rank."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M
NAN = float('nan')

VALUES = [None, 7, 7, 1, 2, 2, 6, 4, 3, 0]


@parameterized.named_parameters(*utils.ARRAY_FACTORIES)
class ArrayDenseRankTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_numeric(self, array_factory):
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3, 0, 4])))
    expected_ascending = array_factory(
        [None, 0, 0, 0, 1, 1, 3, 2, 1, 0], arolla.INT64
    )
    expected_descending = array_factory(
        [None, 0, 0, 1, 0, 0, 0, 1, 2, 3], arolla.INT64
    )
    for dtype in arolla.types.NUMERIC_QTYPES:
      with self.subTest(str(dtype)):
        values = array_factory(VALUES, dtype)
        arolla.testing.assert_qvalue_allequal(
            self.eval(M.array.dense_rank(values, over=edge)),
            expected_ascending,
        )
        arolla.testing.assert_qvalue_allequal(
            self.eval(M.array.dense_rank(values, over=edge, descending=True)),
            expected_descending,
        )

  def test_over_scalar(self, array_factory):
    expected = array_factory([None, 0, 0, 5, 4, 4, 1, 2, 3, 6], arolla.INT64)
    for dtype in arolla.types.NUMERIC_QTYPES:
      with self.subTest(str(dtype)):
        values = array_factory(VALUES, dtype)
        arolla.testing.assert_qvalue_allequal(
            self.eval(M.array.dense_rank(values, descending=True)), expected
        )

  def test_bytes(self, array_factory):
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3, 0, 4])))
    expected_ascending = array_factory(
        [None, 0, 0, 0, 1, 1, 3, 2, 1, 0], arolla.INT64
    )
    expected_descending = array_factory(
        [None, 0, 0, 1, 0, 0, 0, 1, 2, 3], arolla.INT64
    )
    str_values = [str(v).encode() if v is not None else None for v in VALUES]
    values = array_factory(str_values, arolla.BYTES)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.dense_rank(values, edge)), expected_ascending
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.dense_rank(values, edge, descending=True)),
        expected_descending,
    )

  def test_texts(self, array_factory):
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3, 0, 4])))
    expected_ascending = array_factory(
        [None, 0, 0, 0, 1, 1, 3, 2, 1, 0], arolla.INT64
    )
    expected_descending = array_factory(
        [None, 0, 0, 1, 0, 0, 0, 1, 2, 3], arolla.INT64
    )
    str_values = [str(v) if v is not None else None for v in VALUES]
    values = array_factory(str_values, arolla.TEXT)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.dense_rank(values, edge)), expected_ascending
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.dense_rank(values, edge, descending=True)),
        expected_descending,
    )

  def test_floating_with_nan(self, array_factory):
    values = [3, 8, 8, NAN, None, 2, None, NAN, 1, 8]
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3, 0, 4])))
    expected_ascending = array_factory(
        [0, 1, 1, 1, None, 0, None, 2, 0, 1], arolla.INT64
    )
    expected_descending = array_factory(
        [1, 0, 0, 1, None, 0, None, 2, 1, 0], arolla.INT64
    )
    for dtype in arolla.types.FLOATING_POINT_QTYPES:
      with self.subTest(str(dtype)):
        a = array_factory(values, dtype)
        arolla.testing.assert_qvalue_allequal(
            self.eval(M.array.dense_rank(a, over=edge)),
            expected_ascending,
        )
        arolla.testing.assert_qvalue_allequal(
            self.eval(M.array.dense_rank(a, over=edge, descending=True)),
            expected_descending,
        )

  def test_bool(self, array_factory):
    edge = arolla.eval(M.edge.from_sizes(array_factory([3, 3, 0, 2])))
    expected_ascending = array_factory(
        [1, 0, None, 0, None, 0, None, None], arolla.INT64
    )
    expected_descending = array_factory(
        [0, 1, None, 0, None, 0, None, None], arolla.INT64
    )
    values = array_factory([True, False, None, False, None, False, None, None])
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.dense_rank(values, edge)), expected_ascending
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.dense_rank(values, edge, descending=True)),
        expected_descending,
    )


if __name__ == '__main__':
  absltest.main()
