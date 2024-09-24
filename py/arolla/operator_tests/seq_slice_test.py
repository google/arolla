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

"""Tests for seq.slice operator."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

_VALUE_QTYPES = list(pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES)
_SEQUENCE_QTYPES = list(
    arolla.types.make_sequence_qtype(x) for x in _VALUE_QTYPES
)

# All qtypes that we consider existing in the qtype signatures test
_ALL_POSSIBLE_QTYPES = _SEQUENCE_QTYPES + list(
    pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES
)


def gen_qtype_signatures():
  """Yields qtype signatures for seq.slice.

  Yields: (seq_qtype, start_qtype, stop_qtype, result_qtype)
  """
  for sequence_qtype in _SEQUENCE_QTYPES:
    for start_qtype in arolla.types.INTEGRAL_QTYPES:
      for stop_qtype in arolla.types.INTEGRAL_QTYPES:
        yield (sequence_qtype, start_qtype, stop_qtype, sequence_qtype)


def gen_arrays(n):
  for a0 in (-3, 0, 3):
    for count in range(0, n):
      yield range(a0, a0 + count)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())

N = 5


class SeqSliceTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.seq.slice,
        QTYPE_SIGNATURES,
        possible_qtypes=_ALL_POSSIBLE_QTYPES,
    )

  @parameterized.parameters(
      *itertools.product(gen_arrays(N), range(-N, N), range(-N, N)),
      ([1, 6, 4, 3], 1, 3),
      (range(100), 5, -5),
  )
  def testValue(self, values, start, stop):
    seq_a = arolla.types.Sequence(*values, value_qtype=arolla.INT32)
    expected_qvalue = arolla.eval(
        arolla.types.Sequence(*values[start:stop], value_qtype=arolla.INT32)
    )
    actual_qvalue = arolla.eval(M.seq.slice(seq_a, start, stop))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_qvalue, expected_qvalue
    )

  def testComplexQType(self):
    seq_a = arolla.types.Sequence(
        arolla.types.Sequence(1, 2),
        arolla.types.Sequence(3, 4, 5),
        arolla.types.Sequence(6),
        arolla.types.Sequence(7),
    )
    expected_qvalue = arolla.types.Sequence(
        arolla.types.Sequence(3, 4, 5),
        arolla.types.Sequence(6),
        arolla.types.Sequence(7),
    )
    actual_value = arolla.eval(M.seq.slice(seq_a, 1, 4))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_value, expected_qvalue
    )


if __name__ == '__main__':
  absltest.main()
