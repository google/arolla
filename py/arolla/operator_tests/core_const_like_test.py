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

"""Tests for M.core.const_like."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

INF = float("inf")
NAN = float("nan")


TEST_DATA = (
    (None, 2, 2),
    (None, 3, 3),
    (False, False, False),
    (True, False, False),
    (False, 1, True),
    (2, 0, 0),
    (2, True, 1),
    ("a", "b", "b"),
    (b"a", b"b", b"b"),
    (-INF, 0, 0),
    (INF, 0, 0),
    (NAN, 0, 0),
    (arolla.unit(), arolla.unit(), arolla.unit()),
)

CAST_ARG_SCALAR_QTYPES = (
    arolla.BOOLEAN,
    arolla.types.UINT64,
) + arolla.types.NUMERIC_QTYPES


def gen_qtype_signatures():
  """Yields qtype signatures for core.const_like.

  Yields: (arg_qtype, result_qtype)
  """
  input_scalar_qtypes = frozenset(
      tuple(zip(arolla.types.SCALAR_QTYPES, arolla.types.SCALAR_QTYPES))
      + tuple(itertools.product(CAST_ARG_SCALAR_QTYPES, CAST_ARG_SCALAR_QTYPES))
  )
  for target_scalar_qtype, const_scalar_qtype in input_scalar_qtypes:
    target_optional_qtype = arolla.make_optional_qtype(target_scalar_qtype)
    const_optional_qtype = arolla.make_optional_qtype(const_scalar_qtype)
    for target_qtype in pointwise_test_utils.lift_qtypes(target_scalar_qtype):
      yield (target_qtype, const_scalar_qtype, target_qtype)
    for target_qtype in pointwise_test_utils.lift_qtypes(target_optional_qtype):
      yield (target_qtype, const_optional_qtype, target_qtype)
    # Special input types: Scalar + Optional (e.g. INT32, Optional<FLOAT32>)
    yield (target_scalar_qtype, const_optional_qtype, target_optional_qtype)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreConstLikeTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertCountEqual(
        frozenset(QTYPE_SIGNATURES),
        pointwise_test_utils.detect_qtype_signatures(M.core.const_like),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, target, const_value, expected_value):
    actual_value = arolla.eval(M.core.const_like(target, const_value))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == "__main__":
  absltest.main()
