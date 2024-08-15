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

"""Tests for core.where."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_qtype_signatures():
  """Yields qtype signatures for core.where.

  Yields: (arg_1_qtype, arg_2_qtype, arg_3_qtype, result_qtype)
  """
  condition_qtypes = pointwise_test_utils.lift_qtypes(arolla.UNIT)
  value_qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.SCALAR_QTYPES)
  for arg_1, arg_2, arg_3 in itertools.product(
      condition_qtypes, value_qtypes, value_qtypes
  ):
    with contextlib.suppress(arolla.types.QTypeError):
      result = arolla.types.common_qtype(arg_2, arg_3)
      if arolla.types.is_scalar_qtype(arg_1) or arolla.types.is_optional_qtype(
          arg_1
      ):
        yield (arg_1, arg_2, arg_3, result)
      else:
        yield (
            arg_1,
            arg_2,
            arg_3,
            arolla.types.broadcast_qtype([arg_1], result),
        )


def gen_test_data():
  """Yields test data for core.where operator.

  Yields: (arg_1, arg_2, arg_3, result)
  """
  condition_values = (True, False, None)
  values = (None, 'a', 'b', b'a', b'b', 0, 1, True, False)
  for arg_1, arg_2, arg_3 in itertools.product(
      condition_values, values, values
  ):
    yield arg_1, arg_2, arg_3, arg_2 if arg_1 else arg_3


TEST_DATA = tuple(
    pointwise_test_utils.gen_cases(
        tuple(gen_test_data()), *gen_qtype_signatures()
    )
)
# Form the QTYPE_SIGNATURES from the TEST_DATA to force us to have >=1 testcase
# per qtype signature.
QTYPE_SIGNATURES = frozenset(
    tuple(v.qtype for v in testcase) for testcase in TEST_DATA
)


class CoreWhereTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        QTYPE_SIGNATURES,
        frozenset(pointwise_test_utils.detect_qtype_signatures(M.core.where)),
    )

  @parameterized.parameters(*TEST_DATA)
  def test_value(self, arg_1, arg_2, arg_3, expected):
    result = self.eval(M.core.where(arg_1, arg_2, arg_3))
    arolla.testing.assert_qvalue_allequal(result, expected)


if __name__ == '__main__':
  absltest.main()
