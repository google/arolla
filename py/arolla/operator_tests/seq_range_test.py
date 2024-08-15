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

"""Tests for M.seq.range."""

import itertools
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_qtype_signatures():
  """Yields qtype signatures for seq.at.

  Yields: (arg_1_qtype, arg_2_qtype, result_qtype)
  """
  sequence_int64_qtype = arolla.types.make_sequence_qtype(arolla.INT64)
  for arg_1_qtype in arolla.types.INTEGRAL_QTYPES:
    yield (arg_1_qtype, sequence_int64_qtype)
    for arg_2_qtype in arolla.types.INTEGRAL_QTYPES:
      yield (arg_1_qtype, arg_2_qtype, sequence_int64_qtype)
      yield (
          arg_1_qtype,
          arolla.types.make_optional_qtype(arg_2_qtype),
          sequence_int64_qtype,
      )
      for arg_3_qtype in arolla.types.INTEGRAL_QTYPES:
        yield (arg_1_qtype, arg_2_qtype, arg_3_qtype, sequence_int64_qtype)
        yield (
            arg_1_qtype,
            arolla.types.make_optional_qtype(arg_2_qtype),
            arg_3_qtype,
            sequence_int64_qtype,
        )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())

N = 8


class SeqRangeTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(pointwise_test_utils.detect_qtype_signatures(M.seq.range)),
    )

  @parameterized.parameters(range(-N, N))
  def testValue1(self, n):
    expected_output_qvalue = arolla.types.Sequence(
        *map(arolla.int64, range(n)), value_qtype=arolla.INT64
    )
    actual_output_qvalue = arolla.eval(M.seq.range(n))
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )

  @parameterized.parameters(itertools.product(range(-N, N), range(-N, N)))
  def testValue2(self, n, m):
    expected_output_qvalue = arolla.types.Sequence(
        *map(arolla.int64, range(n, m)), value_qtype=arolla.INT64
    )
    actual_output_qvalue = arolla.eval(M.seq.range(n, m))
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )

  @parameterized.parameters(
      *itertools.product(range(-N, N), range(-N, N), range(-3, 0)),
      *itertools.product(range(-N, N), range(-N, N), range(1, 4)),
  )
  def testValue3(self, n, m, k):
    expected_output_qvalue = arolla.types.Sequence(
        *map(arolla.int64, range(n, m, k)), value_qtype=arolla.INT64
    )
    actual_output_qvalue = arolla.eval(M.seq.range(n, m, k))
    arolla.testing.assert_qvalue_allequal(
        actual_output_qvalue, expected_output_qvalue
    )

  def testError(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`step` must be non-zero')
    ):
      _ = arolla.eval(M.seq.range(0, 99, 0))


if __name__ == '__main__':
  absltest.main()
