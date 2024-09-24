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

"""Tests for array.ordinal_rank operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils


M = arolla.M
NAN = float('nan')


def gen_test_cases(array_factory):
  xs0 = [
      array_factory([7, 8, None, 7, 7, 8], value_qtype=qtype)
      for qtype in arolla.types.NUMERIC_QTYPES + (arolla.types.UINT64,)
  ] + [
      array_factory(['a', 'b', None, 'a', 'a', 'b'], arolla.TEXT),
      array_factory([b'a', b'b', None, b'a', b'a', b'b'], arolla.BYTES),
      array_factory([False, True, None, False, False, True], arolla.BOOLEAN),
  ]
  xs1 = [
      array_factory(
          [None, arolla.unit(), None, None, arolla.unit(), arolla.unit()]
      ),
      array_factory([None, False, None, None, False, False], arolla.BOOLEAN),
  ]

  inputs = [xs0, xs1]
  expectations = [
      array_factory([0, 3, None, 1, 2, 4], arolla.INT64),
      array_factory([None, 0, None, None, 1, 2], arolla.INT64),
  ]

  for xs, expected in zip(inputs, expectations):
    for x in xs:
      yield (x, expected)  # call with 1 arg
      tie_breakers = [
          array_factory([3] * len(x), value_qtype=qtype)
          for qtype in arolla.types.INTEGRAL_QTYPES
      ] + [arolla.unspecified()]
      for tie_breaker in tie_breakers:
        yield (x, tie_breaker, expected)  # call with 2 args
        edges = [
            arolla.eval(M.edge.to_single(x)),
            arolla.eval(M.edge.to_scalar(x)),
            arolla.unspecified(),
        ]
        for over in edges:
          yield (x, tie_breaker, over, expected)  # call with 3 args
          descending = arolla.boolean(False)
          yield (x, tie_breaker, over, descending, expected)  # call with 4 args


TEST_CASES = [
    *gen_test_cases(arolla.array),
    *gen_test_cases(arolla.dense_array),
]


class ArrayOrdinalRankTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    expected_qtype_signatures = frozenset(
        tuple(arg.qtype for arg in test_case) for test_case in TEST_CASES
    )
    arolla.testing.assert_qtype_signatures(
        M.array.ordinal_rank, expected_qtype_signatures
    )

  @parameterized.parameters((test_case,) for test_case in TEST_CASES)
  def test_generated_cases(self, test_case):
    args, expected = test_case[:-1], test_case[-1]
    actual = self.eval(M.array.ordinal_rank(*args))
    arolla.testing.assert_qvalue_allequal(actual, expected)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_tie_breaking(self, array_factory):
    x = array_factory(['hello'] * 8)
    tb = array_factory([-1, -2, -3, None, -5, None, -7, -8])
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.ordinal_rank(x, tie_breaker=tb)),
        array_factory([5, 4, 3, None, 2, None, 1, 0], arolla.INT64),
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.ordinal_rank(x, tie_breaker=tb, descending=True)),
        array_factory([5, 4, 3, None, 2, None, 1, 0], arolla.INT64),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_tie_breaking_with_mapping_edge(self, array_factory):
    x = array_factory(['hello'] * 8)
    tb = array_factory([-1, -2, -3, None, -5, None, -7, -8])
    over = arolla.eval(
        M.edge.from_mapping(array_factory([2, 1, 0, 2, 1, 0, 2, 1]), 3)
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.ordinal_rank(x, tie_breaker=tb, over=over)),
        array_factory([1, 2, 0, None, 1, None, 0, 0], arolla.INT64),
    )

  @parameterized.parameters(
      *itertools.product(
          [arolla.array, arolla.dense_array], arolla.types.NUMERIC_QTYPES
      )
  )
  def test_descending_order(self, array_factory, value_qtype):
    x = array_factory([3, 4, 5, 5, 4, 3, 4], value_qtype)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.ordinal_rank(x, descending=True)),
        array_factory([5, 2, 0, 1, 3, 6, 4], arolla.INT64),
    )

    tie_breaker = array_factory([20, 20, 20, 10, 10, 10, 10], arolla.INT32)
    arolla.testing.assert_qvalue_allequal(
        self.eval(
            M.array.ordinal_rank(x, tie_breaker=tie_breaker, descending=True)
        ),
        array_factory([6, 4, 1, 0, 2, 5, 3], arolla.INT64),
    )

    over = arolla.eval(M.edge.from_sizes(array_factory([4, 3])))
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.ordinal_rank(x, over=over, descending=True)),
        array_factory([3, 2, 0, 1, 0, 2, 1], arolla.INT64),
    )

  @parameterized.parameters(
      *itertools.product(
          [arolla.array, arolla.dense_array], arolla.types.SCALAR_QTYPES
      )
  )
  def test_empty(self, array_factory, value_qtype):
    x = array_factory([], value_qtype)
    empty_i64 = array_factory([], arolla.INT64)
    empty_i32 = array_factory([], arolla.INT32)
    for tie_breaker in [empty_i32, empty_i64]:
      for over in [
          M.edge.from_mapping(empty_i64, 1),
          M.edge.from_mapping(empty_i64, 2),
      ]:
        for descending in [True, False]:
          expr = M.array.ordinal_rank(
              x, tie_breaker=tie_breaker, over=over, descending=descending
          )
          arolla.testing.assert_qvalue_allequal(
              self.eval(expr), array_factory([], arolla.INT64)
          )

  @parameterized.parameters(
      *itertools.product(
          [arolla.array, arolla.dense_array], arolla.types.FLOATING_POINT_QTYPES
      )
  )
  def test_with_nan(self, array_factory, value_qtype):
    x = array_factory([3, 1, NAN, 5, NAN, 2], value_qtype)
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.ordinal_rank(x)),
        array_factory([2, 0, 4, 3, 5, 1], arolla.INT64),
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.ordinal_rank(x, descending=True)),
        array_factory([1, 3, 4, 0, 5, 2], arolla.INT64),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_chained_tie_breaking(self, array_factory):
    # Rank by a0, resolve ties by a1, then by a2.
    a0 = array_factory(['a', 'b', 'a', 'b', 'a', 'b'])
    a1 = array_factory([b'y', b'y', b'y', b'x', b'x', b'x'])
    a2 = array_factory([6, 5, 4, None, 2, 1], arolla.INT64)

    temp = self.eval(M.array.ordinal_rank(a1, tie_breaker=a2))
    arolla.testing.assert_qvalue_allequal(
        temp, array_factory([4, 3, 2, None, 1, 0], arolla.INT64)
    )
    rank = self.eval(M.array.ordinal_rank(a0, tie_breaker=temp))
    arolla.testing.assert_qvalue_allequal(
        rank, array_factory([2, 4, 1, None, 0, 3], arolla.INT64)
    )


if __name__ == '__main__':
  absltest.main()
