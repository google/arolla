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

"""Tests for the M.array.expand operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import utils

M = arolla.M


def gen_test_data(*, sorted_mapping):
  """Yields test data for array.expand.

  Args:
    sorted_mapping: whether the edge should be built from a sorted mapping
      (compatible with split_points) or not.

  Yields: (arg_1, arg_2, result)
  """
  expand = lambda x, mapping: [x[i] for i in mapping]
  for array_mod in (arolla.dense_array, arolla.array):
    # Array input - array output.
    for data, value_qtype in itertools.chain(
        [
            ([0, 1, 2], qt)
            for qt in arolla.types.NUMERIC_QTYPES + (arolla.types.UINT64,)
        ],
        [
            ([None, True, True], arolla.UNIT),
            ([None, True, False], arolla.BOOLEAN),
            ([b'a', b'b', b'c'], arolla.BYTES),
            (['a', 'b', 'c'], arolla.TEXT),
        ],
    ):
      arolla_data = array_mod(data, value_qtype=value_qtype)
      # Unsorted mapping -> not possible to require split_point representation.
      if sorted_mapping:
        mapping = [0, 0, 0, 1, 2, 2]
      else:
        mapping = [0, 0, 1, 0, 2, 2]
      res = array_mod(expand(data, mapping), value_qtype=value_qtype)
      yield arolla_data, arolla.eval(
          M.edge.from_mapping(array_mod(mapping), 3)
      ), res

    # Scalar input - array output.
    for (_, scalar_value), sizes in itertools.product(
        utils.SCALAR_VALUES + utils.OPTIONAL_VALUES, ([], [1], [2, 0, 1, 2])
    ):
      times = sum(sizes)
      res = array_mod(
          list(itertools.repeat(scalar_value, times)),
          value_qtype=arolla.types.get_scalar_qtype(scalar_value.qtype),
      )
      yield scalar_value, arolla.eval(
          M.edge.from_shape(M.core.shape_of(res))
      ), res

  # Scalar input - scalar output.
  for _, scalar_value in utils.SCALAR_VALUES + utils.OPTIONAL_VALUES:
    yield scalar_value, arolla.types.ScalarToScalarEdge(), scalar_value


SORTED_TEST_DATA = tuple(gen_test_data(sorted_mapping=True))
UNSORTED_TEST_DATA = tuple(gen_test_data(sorted_mapping=False))
QTYPE_SIGNATURES = frozenset(
    (arg_1.qtype, arg_2.qtype, res.qtype)
    for arg_1, arg_2, res in SORTED_TEST_DATA
)


class ArrayExpandTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.expand, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*SORTED_TEST_DATA)
  def testValueWithSortedMapping(self, arg1, arg2, expected_result):
    result = self.eval(M.array.expand(arg1, arg2))
    arolla.testing.assert_qvalue_allequal(result, expected_result)

  @parameterized.parameters(*UNSORTED_TEST_DATA)
  def testValueWithSortedUnsortedMapping(self, arg1, arg2, expected_result):
    result = self.eval(M.array.expand(arg1, arg2))
    arolla.testing.assert_qvalue_allequal(result, expected_result)

  def testArrayAndScalarToScalarEdgeErrorMsg(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError,
        (
            'ARRAY_FLOAT32 is not compatible with parent-side of over:'
            ' SCALAR_TO_SCALAR_EDGE'
        ),
    ):
      M.array.expand(
          arolla.array_float32([1.0, 2, 3]), arolla.types.ScalarToScalarEdge()
      )


if __name__ == '__main__':
  absltest.main()
