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

"""Tests for M.qtype.slice_tuple_qtype operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M


def gen_test_cases():
  def _gen(input_qtype, input_offset, input_size, expected_qtype):
    for fn in (arolla.int32, arolla.int64):
      for gn in (arolla.int32, arolla.int64):
        yield (input_qtype, fn(input_offset), gn(input_size), expected_qtype)

  empty_tuple_qtype = arolla.tuple().qtype
  yield from (
      *_gen(empty_tuple_qtype, 0, 0, empty_tuple_qtype),
      *_gen(empty_tuple_qtype, 0, 1, arolla.NOTHING),
      *_gen(empty_tuple_qtype, 0, -1, empty_tuple_qtype),
      *_gen(empty_tuple_qtype, 0, -2, arolla.NOTHING),
      *_gen(empty_tuple_qtype, 1, 0, arolla.NOTHING),
      *_gen(empty_tuple_qtype, 1, -1, arolla.NOTHING),
      *_gen(empty_tuple_qtype, -1, 0, arolla.NOTHING),
      *_gen(empty_tuple_qtype, -1, -1, arolla.NOTHING),
  )
  nothing_tuple_qtype = arolla.make_tuple_qtype(arolla.NOTHING)
  yield from (
      *_gen(nothing_tuple_qtype, 0, 1, nothing_tuple_qtype),
      *_gen(nothing_tuple_qtype, 0, -1, nothing_tuple_qtype),
      *_gen(nothing_tuple_qtype, 0, 0, empty_tuple_qtype),
      *_gen(nothing_tuple_qtype, 1, 0, empty_tuple_qtype),
      *_gen(nothing_tuple_qtype, 1, -1, empty_tuple_qtype),
      *_gen(nothing_tuple_qtype, 0, 2, arolla.NOTHING),
      *_gen(nothing_tuple_qtype, 1, 1, arolla.NOTHING),
  )
  values = (True, 1, 1.5)
  values_tuple_qtype = arolla.tuple(*values).qtype
  for offset in range(4):
    for size in range(4):
      if offset <= len(values) and offset + size <= len(values):
        yield from _gen(
            values_tuple_qtype,
            offset,
            size,
            arolla.tuple(*values[offset:][:size]).qtype,
        )
      else:
        yield from _gen(
            arolla.tuple(*values).qtype, offset, size, arolla.NOTHING
        )


TEST_CASES = tuple(gen_test_cases())
QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case) for test_case in TEST_CASES
)


class QTypeMakeTupleQType(parameterized.TestCase):

  def test_qtype_signature(self):
    self.assertCountEqual(
        pointwise_test_utils.detect_qtype_signatures(M.qtype.slice_tuple_qtype),
        QTYPE_SIGNATURES,
    )

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    self.assertEqual(
        arolla.eval(M.qtype.slice_tuple_qtype(*args)), expected_result
    )


if __name__ == '__main__':
  absltest.main()
