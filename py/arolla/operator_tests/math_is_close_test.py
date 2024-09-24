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

"""Tests for M.math.is_close operator."""

import contextlib
import itertools
import random

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils
import numpy as np


NAN = float('nan')
INF = float('inf')

M = arolla.M


def gen_test_data():
  """Yields test data in from (x, y, rtol=, atol=, expected_value)."""

  yield (1, 1, True)
  yield (1, 2, None)
  yield (NAN, 1, None)
  yield (NAN, NAN, None)
  yield (INF, INF, True)
  yield (INF, -INF, None)

  # Missing values handling.
  yield (None, 1, None)
  yield (None, None, None)

  # Default rtol.
  yield (1, 1 + 1e-6, True)
  yield (1, 1 + 1e-4, None)

  # Default atol.
  yield (1e-5, 1e-5 + 1e-9, True)
  yield (1e-5, 1e-5 + 1e-7, None)

  # Only relative tolerance.
  yield (1, 1 + 1e-7, 1e-6, 0, True)
  yield (1, 1 + 1e-7, 1e-8, 0, None)

  # Only absolute tolerance.
  yield (1, 1 + 1e-7, 0, 1e-6, True)
  yield (1, 1 + 1e-7, 0, 1e-8, None)

  # Both tolerances set together.
  yield (1, 1 + 1e-6, 1e-6, 1e-7, True)
  yield (1, 1 + 1e-9, 1e-9, 1e-7, True)

  # Int cases.
  yield (10, 11, 0, None)
  yield (10, 11, 0, 1, True)

  int32_min, int32_max = arolla.int32(-(2**31)), arolla.int32(2**31 - 1)
  int64_min, int64_max = arolla.int64(-(2**63)), arolla.int64(2**63 - 1)

  # No integer overflow.
  yield (int32_min, int32_max, None)
  yield (int64_min, int64_max, None)

  # Big ints.
  yield (int32_min, int32_min + 1, True)
  yield (int64_min, int64_min + 1, True)
  yield (int32_max, int32_max - 1, True)
  yield (int64_max, int64_max - 1, True)

  # The computation is done in float64 so absolute tolerance is insufficient
  # to distinguish between two big int64s.
  yield (int32_min, int32_min + 2, 0, 1, None)
  yield (int32_max, int32_max - 2, 0, 1, None)
  yield (int64_min, int64_min + 2, 0, 1, True)
  yield (int64_max, int64_max - 2, 0, 1, True)


def gen_qtype_signatures():
  qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.NUMERIC_QTYPES)
  for arity in [2, 3, 4]:
    for args in itertools.product(qtypes, repeat=arity):
      with contextlib.suppress(arolla.types.QTypeError):
        yield (*args, arolla.types.broadcast_qtype(args, arolla.OPTIONAL_UNIT))


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


# Limit the number of eval test cases to ensure the test completes within
# a reasonable timeframe.
TEST_CASES = random.Random(42).sample(
    list(pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)),
    10_000,
)


def parse_test_case(test_case):
  """Parses a test case into (*args, kwargs, expected)."""
  kwargs = {}
  if len(test_case) == 5:
    x, y, kwargs['rtol'], kwargs['atol'], expected = test_case
  elif len(test_case) == 4:
    x, y, kwargs['rtol'], expected = test_case
  else:
    x, y, expected = test_case
  return x, y, kwargs, expected


class MathIsCloseTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(M.math.is_close, QTYPE_SIGNATURES)

  @parameterized.parameters(*TEST_DATA)
  def test_test_data(self, *test):
    """Verifies that test data matches numpy.isclose behavior."""
    x, y, kwargs, expected = parse_test_case(test)
    if x is None or y is None:
      return
    if expected is None:
      expected = False
    self.assertEqual(np.isclose(float(x), float(y), **kwargs), expected)

  @parameterized.parameters(TEST_CASES)
  def test_value(self, *test):
    x, y, kwargs, expected = parse_test_case(test)

    actual = arolla.eval(M.math.is_close(x, y, **kwargs))
    arolla.testing.assert_qvalue_allequal(actual, expected)

    # Test commutativity.
    actual = arolla.eval(M.math.is_close(y, x, **kwargs))
    arolla.testing.assert_qvalue_allequal(actual, expected)


if __name__ == '__main__':
  absltest.main()
