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
import math

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

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
  yield (1, None, None)
  yield (None, None, None)

  # Default tolerance.
  yield (arolla.float32(1.0), arolla.float32(1 + 1e-7), True)
  yield (arolla.float32(1.0), arolla.float32(1 + 1e-5), None)
  yield (1, arolla.float64(1 + 1e-10), True)
  yield (1, arolla.float64(1 + 1e-8), None)

  # Only relative tolerance.
  yield (1, 1 + 1e-7, 1e-6, True)
  yield (1, 1 + 1e-7, 1e-8, None)

  # Only absolute tolerance.
  yield (1, 1 + 1e-7, 0, 1e-6, True)
  yield (1, 1 + 1e-7, 0, 1e-8, None)

  # Both tolerances set together.
  yield (1, 1 + 1e-6, 1e-6, 1e-7, True)
  yield (1, 1 + 1e-9, 1e-9, 1e-7, True)

  # Additional int cases.
  yield (10, 11, 0, None)
  yield (10, 11, 0, 1, True)


def gen_qtype_signatures(include_unspecified=True):
  def with_result_type(*args):
    with contextlib.suppress(arolla.types.QTypeError):
      result_qtype = arolla.types.broadcast_qtype(
          [a for a in args if a != arolla.UNSPECIFIED], arolla.OPTIONAL_UNIT
      )
      yield (*args, result_qtype)

  xy_qtypes = pointwise_test_utils.lift_qtypes(
      *arolla.types.FLOATING_POINT_QTYPES
  )
  atol_qtypes = pointwise_test_utils.lift_qtypes(
      *arolla.types.NUMERIC_QTYPES
  )
  rtol_qtypes = atol_qtypes
  if include_unspecified:
    rtol_qtypes += (arolla.UNSPECIFIED,)
  for args in itertools.product(xy_qtypes, repeat=2):
    yield from with_result_type(*args)
    for rtol in rtol_qtypes:
      yield from with_result_type(*args, rtol)
      for atol in atol_qtypes:
        yield from with_result_type(*args, rtol, atol)


TEST_DATA = tuple(gen_test_data())
QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class CoreEqualTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.math.is_close,
        QTYPE_SIGNATURES,
        # Detecting signatures for 4-ary operator takes too much time,
        # so we limit the set of possible argument.
        possible_qtypes=pointwise_test_utils.lift_qtypes(
            *arolla.types.NUMERIC_QTYPES, arolla.UNIT
        )
        + (arolla.UNSPECIFIED,),
    )

  @parameterized.parameters(*TEST_DATA)
  def test_test_data(self, *test):
    """Verifies that test data matches math.isclose behavior."""
    rtol = 1e-9
    atol = 0
    if len(test) == 5:
      x, y, rtol, atol, expected_value = test
    elif len(test) == 4:
      x, y, rtol, expected_value = test
    else:
      x, y, expected_value = test

    if not all(isinstance(v, float) for v in [x, y, rtol]):
      return

    if expected_value is None:
      expected_value = False

    self.assertEqual(
        math.isclose(x, y, abs_tol=atol, rel_tol=rtol), expected_value
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *gen_qtype_signatures(False))
  )
  def test_value(self, *test):

    def test_eval(x, y, expected_value, **kwargs):
      actual_value = arolla.eval(M.math.is_close(x, y, **kwargs))
      arolla.testing.assert_qvalue_allequal(actual_value, expected_value)

    kwargs = {}
    if len(test) == 5:
      x, y, kwargs['rtol'], kwargs['atol'], expected_value = test
      test_eval(x, y, expected_value, **kwargs)
    elif len(test) == 4:
      x, y, kwargs['rtol'], expected_value = test
      test_eval(x, y, expected_value, **kwargs)
    else:
      x, y, expected_value = test
      test_eval(x, y, expected_value, **kwargs)
      kwargs['rtol'] = arolla.unspecified()
      test_eval(x, y, expected_value, **kwargs)


if __name__ == '__main__':
  absltest.main()
