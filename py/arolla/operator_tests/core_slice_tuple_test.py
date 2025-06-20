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

import re
from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

L = arolla.L
M = arolla.M


def gen_test_cases():

  def _gen(input_, input_offset, input_size, expected):
    for fn in (
        arolla.int32,
        arolla.int64,
        arolla.optional_int32,
        arolla.optional_int64,
    ):
      for gn in (
          arolla.int32,
          arolla.int64,
          arolla.optional_int32,
          arolla.optional_int64,
      ):
        yield (input_, fn(input_offset), gn(input_size), expected)

  empty_tuple = arolla.tuple()
  yield from (
      *_gen(empty_tuple, 0, 0, empty_tuple),
      *_gen(empty_tuple, 0, 1, 'out of bounds'),
      *_gen(empty_tuple, 0, -1, empty_tuple),
      *_gen(empty_tuple, 0, -2, 'out of bounds'),
      *_gen(empty_tuple, 1, 0, 'out of bounds'),
      *_gen(empty_tuple, 1, -1, 'out of bounds'),
      *_gen(empty_tuple, -1, 0, 'out of bounds'),
      *_gen(empty_tuple, -1, -1, 'out of bounds'),
  )
  just_zero_tuple = arolla.tuple(0)
  yield from (
      *_gen(just_zero_tuple, 0, 1, just_zero_tuple),
      *_gen(just_zero_tuple, 0, -1, just_zero_tuple),
      *_gen(just_zero_tuple, 0, 0, empty_tuple),
      *_gen(just_zero_tuple, 1, 0, empty_tuple),
      *_gen(just_zero_tuple, 1, -1, empty_tuple),
      *_gen(
          just_zero_tuple, 0, 2, 'out of bounds: offset=0, size=2, tuple_size=1'
      ),
      *_gen(just_zero_tuple, 1, 1, 'out of bounds'),
  )
  values = (True, 1, 1.5)
  values_tuple = arolla.tuple(*values)
  for offset in range(4):
    for size in range(4):
      if offset <= len(values) and offset + size <= len(values):
        yield from _gen(
            values_tuple,
            offset,
            size,
            arolla.tuple(*values[offset:][:size]),
        )
      else:
        yield from _gen(values_tuple, offset, size, 'out of bounds')


TEST_CASES = tuple(gen_test_cases())
QTYPE_SIGNATURES = frozenset(
    tuple(x.qtype for x in test_case)
    for test_case in TEST_CASES
    if not isinstance(test_case[-1], str)
)


class CoreSliceTupleTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_CASES)
  def test_eval(self, *test_case):
    tuple_, offset, size, expected_result = test_case
    if isinstance(expected_result, str):
      with self.assertRaisesRegex(ValueError, expected_result):
        _ = arolla.eval(M.core.slice_tuple(L.tuple, offset, size), tuple=tuple_)
    else:
      arolla.testing.assert_qvalue_equal_by_fingerprint(
          arolla.eval(M.core.slice_tuple(L.tuple, offset, size), tuple=tuple_),
          expected_result,
      )

  def test_error_non_integral_offset(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an integer, got offset: FLOAT32')
    ):
      _ = M.core.slice_tuple(arolla.tuple(1, 2, 3), 2.0, 0)

  def test_error_missing_offset(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an integer, got offset=missing')
    ):
      _ = M.core.slice_tuple(
          arolla.tuple(1, 2, 3), arolla.optional_int64(None), 0
      )

  def test_error_non_literal_offset(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`offset` must be literal')
    ):
      _ = M.core.slice_tuple(arolla.L.x, arolla.literal(1) + 2, -1)

  def test_error_non_integral_size(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an integer, got size: FLOAT32')
    ):
      _ = M.core.slice_tuple(arolla.tuple(1, 2, 3), 0, 2.0)

  def test_error_missing_size(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected an integer, got size=missing')
    ):
      _ = M.core.slice_tuple(
          arolla.tuple(1, 2, 3), 0, arolla.optional_int64(None)
      )

  def test_error_non_literal_size(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`size` must be literal')
    ):
      _ = M.core.slice_tuple(arolla.L.x, 0, arolla.literal(1) + 2)

  def test_namedtuple(self):
    res = arolla.eval(
        M.core.slice_tuple(arolla.namedtuple(a=1, b=2, c=3), 1, 1)
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(res, arolla.tuple(2))

  def test_non_compound_type(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a tuple or a namedtuple, got FLOAT32')
    ):
      _ = arolla.eval(M.core.slice_tuple(1.5, 0, 0))

  def test_unsupported_compound_type(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a tuple or a namedtuple, got OPTIONAL_INT32'),
    ):
      _ = arolla.eval(M.core.slice_tuple(arolla.optional_int32(0), 0, 0))


if __name__ == '__main__':
  absltest.main()
