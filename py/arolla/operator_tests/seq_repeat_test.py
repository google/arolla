# Copyright 2025 Google LLC
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

"""Tests for seq.repeat operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M


class SeqRepeatTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    expected_qtype_signatures = []
    for value_qtype in arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES:
      if value_qtype != arolla.NOTHING:
        for n_qtype in arolla.types.INTEGRAL_QTYPES:
          result_qtype = arolla.types.make_sequence_qtype(value_qtype)
          expected_qtype_signatures.append((value_qtype, n_qtype, result_qtype))
    arolla.testing.assert_qtype_signatures(
        M.seq.repeat,
        expected_qtype_signatures,
    )

  @parameterized.parameters(
      (arolla.unit(), arolla.int32(-10)),
      (arolla.unit(), arolla.int32(0)),
      (arolla.unit(), arolla.int32(10)),
      (arolla.tuple(), arolla.int64(-10)),
      (arolla.tuple(1), arolla.int64(0)),
      (arolla.tuple(1, 2.5), arolla.int64(10)),
      (arolla.dense_array_int32([1, None]), arolla.int32(5)),
  )
  def test_eval(self, value, n):
    expected_result = arolla.types.Sequence(
        *([value] * int(n)), value_qtype=value.qtype
    )
    actual_result = arolla.eval(M.seq.repeat(L.value, L.n), value=value, n=n)
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_result, expected_result
    )


if __name__ == '__main__':
  absltest.main()
