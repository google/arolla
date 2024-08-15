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

"""Tests for core._short_circuit_where."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_qtype_signatures():
  condition_qtypes = (arolla.UNIT, arolla.OPTIONAL_UNIT)
  value_qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.SCALAR_QTYPES)
  for arg_1, arg_2, arg_3 in itertools.product(
      condition_qtypes, value_qtypes, value_qtypes
  ):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (arg_1, arg_2, arg_3, arolla.types.common_qtype(arg_2, arg_3))


QTYPE_SIGNATURES = frozenset(gen_qtype_signatures())


class CoreShortCircuitWhereTest(parameterized.TestCase):

  # Note: The behavior is tested in
  #   arolla/expr/eval/compile_where_operator_test.cc
  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(M.core._short_circuit_where),
        QTYPE_SIGNATURES,
    )


if __name__ == '__main__':
  absltest.main()
