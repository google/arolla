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

"""Tests for edge.group_by."""

import dataclasses
import itertools
from typing import Any

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M
INF = float('inf')


@dataclasses.dataclass
class EdgeGroupByTestParameters:
  x: list[Any]
  edge_sizes: list[int]
  groups_with_default_edge: list[int | None]
  parent_size_with_default_edge: int
  groups_with_edge: list[int | None]
  parent_size_with_edge: int
  qtype: arolla.QType


def gen_test_data():
  integral_data = list()
  for qtype in arolla.types.INTEGRAL_QTYPES:
    integral_data.append(
        EdgeGroupByTestParameters(
            x=[-1, None, 2, 1, 1, None, None, 2, 1],
            edge_sizes=[2, 0, 2, 5],
            groups_with_default_edge=[0, None, 1, 2, 2, None, None, 1, 2],
            parent_size_with_default_edge=3,
            groups_with_edge=[0, None, 1, 2, 3, None, None, 4, 3],
            parent_size_with_edge=5,
            qtype=qtype,
        )
    )
  float_data = list()
  for qtype in arolla.types.FLOATING_POINT_QTYPES:
    float_data.append(
        EdgeGroupByTestParameters(
            x=[-1, None, 2, -1, 5, INF, 6, 3.0, INF],
            edge_sizes=[2, 0, 2, 5],
            groups_with_default_edge=[0, None, 1, 0, 2, 3, 4, 5, 3],
            parent_size_with_default_edge=6,
            groups_with_edge=[0, None, 1, 2, 3, 4, 5, 6, 4],
            parent_size_with_edge=7,
            qtype=qtype,
        )
    )
  unit_data = [
      EdgeGroupByTestParameters(
          x=[True, None, True, True, True, None, None, True, True],
          edge_sizes=[2, 0, 2, 5],
          groups_with_default_edge=[0, None, 0, 0, 0, None, None, 0, 0],
          parent_size_with_default_edge=1,
          groups_with_edge=[0, None, 1, 1, 2, None, None, 2, 2],
          parent_size_with_edge=3,
          qtype=arolla.types.UNIT,
      )
  ]
  boolean_data = [
      EdgeGroupByTestParameters(
          x=[False, None, True, False, False, None, None, True, False],
          edge_sizes=[2, 0, 2, 5],
          groups_with_default_edge=[0, None, 1, 0, 0, None, None, 1, 0],
          parent_size_with_default_edge=2,
          groups_with_edge=[0, None, 1, 2, 3, None, None, 4, 3],
          parent_size_with_edge=5,
          qtype=arolla.types.BOOLEAN,
      )
  ]
  byte_data = [
      EdgeGroupByTestParameters(
          x=[b'a', None, b'b', b'a', b'a', None, None, b'b', b'a'],
          edge_sizes=[2, 0, 2, 5],
          groups_with_default_edge=[0, None, 1, 0, 0, None, None, 1, 0],
          parent_size_with_default_edge=2,
          groups_with_edge=[0, None, 1, 2, 3, None, None, 4, 3],
          parent_size_with_edge=5,
          qtype=arolla.types.BYTES,
      )
  ]
  text_data = [
      EdgeGroupByTestParameters(
          x=['a', None, 'b', 'a', 'a', None, None, 'b', 'a'],
          edge_sizes=[2, 0, 2, 5],
          groups_with_default_edge=[0, None, 1, 0, 0, None, None, 1, 0],
          parent_size_with_default_edge=2,
          groups_with_edge=[0, None, 1, 2, 3, None, None, 4, 3],
          parent_size_with_edge=5,
          qtype=arolla.types.TEXT,
      )
  ]

  for array_fn_name, array_fn in (
      ('dense_array', arolla.dense_array),
      ('array', arolla.array),
  ):
    for test_params in itertools.chain(
        integral_data, float_data, unit_data, boolean_data, byte_data, text_data
    ):
      # Array output, with default edge
      x = array_fn(test_params.x, value_qtype=test_params.qtype)
      yield (
          f'{array_fn_name}_{test_params.qtype}_default_edge',
          x,
          None,
          array_fn(
              test_params.groups_with_default_edge,
              value_qtype=arolla.types.INT64,
          ),
          test_params.parent_size_with_default_edge,
      )

      # Array output, with edge
      x = array_fn(test_params.x, value_qtype=test_params.qtype)
      edge = arolla.eval(M.edge.from_sizes(array_fn(test_params.edge_sizes)))
      yield (
          f'{array_fn_name}_{test_params.qtype}_with_edge',
          x,
          edge,
          array_fn(
              test_params.groups_with_edge, value_qtype=arolla.types.INT64
          ),
          test_params.parent_size_with_edge,
      )

      # Tuple output
      x = array_fn(test_params.x, value_qtype=test_params.qtype)
      y = arolla.eval(
          M.edge.mapping(M.edge.from_sizes(array_fn(test_params.edge_sizes)))
      )
      yield (
          f'{array_fn_name}_{test_params.qtype}_by_tuple',
          M.core.make_tuple(x, y),
          None,
          array_fn(
              test_params.groups_with_edge, value_qtype=arolla.types.INT64
          ),
          test_params.parent_size_with_edge,
      )

      # Tuple output, over edge
      x = array_fn(test_params.x, value_qtype=test_params.qtype)
      edge = arolla.eval(M.edge.from_sizes(array_fn(test_params.edge_sizes)))
      yield (
          f'{array_fn_name}_{test_params.qtype}_by_tuple_with_edge',
          M.core.make_tuple(x, x),
          edge,
          array_fn(
              test_params.groups_with_edge, value_qtype=arolla.types.INT64
          ),
          test_params.parent_size_with_edge,
      )


TEST_DATA = tuple(gen_test_data())


class EdgeGroupByTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  @parameterized.named_parameters(*TEST_DATA)
  def test_all_array_qtype_inputs(
      self, values, edge, expected_mapping, expected_parent_size
  ):
    if edge is None:
      result = self.eval(M.edge.group_by(values))
    else:
      result = self.eval(M.edge.group_by(values, edge))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.mapping(result)), expected_mapping
    )
    self.assertEqual(result.parent_size, expected_parent_size)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_by_tuple_with_sparse_arrays(self, array_factory):
    values = M.core.make_tuple(
        array_factory([0, 0, 0, 0, None, None]),
        array_factory([0, 0, None, None, 0, 0]),
        array_factory(['a', 'b', 'a', None, 'a', None]),
    )
    edge_mapping = array_factory([0, None, 0, 1, None, None], arolla.INT64)
    edge = arolla.eval(M.edge.from_mapping(edge_mapping, parent_size=2))
    expected_mapping = array_factory(
        [0, None, None, None, None, None], arolla.INT64
    )
    expected_parent_size = 1

    result = self.eval(M.edge.group_by(values, edge))
    arolla.testing.assert_qvalue_allequal(
        arolla.eval(M.edge.mapping(result)), expected_mapping
    )
    self.assertEqual(result.parent_size, expected_parent_size)

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_by_tuple_with_different_sizes(self, array_factory):
    values = M.core.make_tuple(
        array_factory([1, 1, 3, 3, None, 1]), array_factory([2, 1, 3])
    )

    with self.assertRaisesRegex(ValueError, 'argument sizes mismatch'):
      _ = self.eval(M.edge.group_by(values))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_by_tuple_with_scalars(self, array_factory):
    values = M.core.make_tuple(array_factory([1, 1, 3, 3, None, 1]), 1)

    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(ValueError, 'unsupported argument types'):
      _ = self.eval(M.edge.group_by(values))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_with_wrong_size_of_edge(self, array_factory):
    values = array_factory([1, 1, 3, 3, None, 1])
    splits = array_factory([0, 3, 8])
    edge = arolla.eval(M.edge.from_split_points(splits))

    with self.assertRaisesRegex(ValueError, 'argument sizes mismatch'):
      _ = self.eval(M.edge.group_by(values, edge))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_nan(self, array_factory):
    values = array_factory([float('nan'), 1.0, 1.0, float('nan')])

    with self.assertRaisesRegex(
        ValueError, 'unable to compute edge.group_by, NaN key is not allowed'
    ):
      _ = self.eval(M.edge.group_by(values))

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_nan_in_tuple(self, array_factory):
    values = M.core.make_tuple(
        array_factory([0, 0, 0, 0, None, None]),
        array_factory([1.0, float('nan'), None, None, 1.0, 0.0]),
    )

    with self.assertRaisesRegex(
        ValueError, 'unable to compute edge.group_by, NaN key is not allowed'
    ):
      _ = self.eval(M.edge.group_by(values))


if __name__ == '__main__':
  absltest.main()
