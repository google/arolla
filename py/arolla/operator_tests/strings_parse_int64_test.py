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

"""Tests for M.strings.parse_int64."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import strings_parse_int_data

M = arolla.M

_TEST_DATA = strings_parse_int_data.TEST_DATA + (
    ('10000000000', 10**10),
    ('1000000000000', 10**12),
    ('100000000000000', 10**14),
    ('10000000000000000', 10**16),
    ('1000000000000000000', 10**18),
)
_TEST_DATA = _TEST_DATA + tuple(
    (None if x[0] is None else x[0].encode('utf8'),) + x[1:] for x in _TEST_DATA
)
_ERROR_TEST_DATA = strings_parse_int_data.ERROR_TEST_DATA + (
    str(2**63),
    str(-(2**63) - 1),
)
_ERROR_TEST_DATA = tuple((s, s) for s in _ERROR_TEST_DATA) + tuple(
    (s, s.encode('utf8')) for s in _ERROR_TEST_DATA
)


def repr_like_absl(s):
  """String repr, but looks more like absl::Utf8SafeCHexEscape."""
  r = repr(s)
  if r.startswith('"'):
    return r.replace("'", "\\'").replace('"', "'")
  return r


class StringsParseInt64Test(parameterized.TestCase):

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          _TEST_DATA,
          *pointwise_test_utils.lift_qtypes(
              (arolla.BYTES, arolla.INT64), (arolla.TEXT, arolla.INT64)
          ),
      )
  )
  def testValue(self, arg, expected_value):
    actual_value = arolla.eval(M.strings.parse_int64(arg))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(
          _ERROR_TEST_DATA,
          *pointwise_test_utils.lift_qtypes(
              (None, arolla.BYTES), (None, arolla.TEXT)
          ),
      )
  )
  def testError(self, s, arg):
    with self.assertRaisesRegex(
        ValueError, re.escape('INT64: ' + repr_like_absl(s))
    ):
      _ = arolla.eval(M.strings.parse_int64(arg))


if __name__ == '__main__':
  absltest.main()
