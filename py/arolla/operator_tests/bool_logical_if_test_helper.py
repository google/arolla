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

"""Tests for M.bool.logical_if operator."""

import contextlib
import itertools

from arolla import arolla
from arolla.operator_tests import pointwise_test_utils


def gen_test_data():
  """Yields test data for bool.logical_if operator.

  Yields: (arg_1, arg_2, arg_3, arg_4, result)
  """
  for arg_2, arg_3, arg_4 in itertools.product([None, False, True], repeat=3):
    yield (True, arg_2, arg_3, arg_4, arg_2)
    yield (False, arg_2, arg_3, arg_4, arg_3)
    yield (None, arg_2, arg_3, arg_4, arg_4)
  for a_2, a_3, a_4 in [
      (b'abc', b'ijk', b'xyz'),
      ('abc', 'ijk', 'xyz'),
      (0, 1, 2),
  ]:
    for mask in range(1, 8):
      arg_2 = a_2 if (mask & 1) else None
      arg_3 = a_3 if (mask & 2) else None
      arg_4 = a_4 if (mask & 3) else None
      yield (True, arg_2, arg_3, arg_4, arg_2)
      yield (False, arg_2, arg_3, arg_4, arg_3)
      yield (None, arg_2, arg_3, arg_4, arg_4)


def gen_qtype_signatures():
  """Yields qtype signatures for bool.logical_if.

  Yields: (arg_1_qtype, arg_2_qtype, arg_3_qtype, arg_4_qtype, result_qtype)
  """
  value_qtypes = pointwise_test_utils.lift_qtypes(*arolla.types.SCALAR_QTYPES)
  for arg_1 in pointwise_test_utils.lift_qtypes(arolla.BOOLEAN):
    for arg_2, arg_3, arg_4 in itertools.product(value_qtypes, repeat=3):
      with contextlib.suppress(arolla.types.QTypeError):
        result = arolla.types.common_qtype(arg_2, arg_3, arg_4)
        if arolla.types.is_scalar_qtype(
            arg_1
        ) or arolla.types.is_optional_qtype(arg_1):
          yield (arg_1, arg_2, arg_3, arg_4, result)
        else:
          yield (
              arg_1,
              arg_2,
              arg_3,
              arg_4,
              arolla.types.broadcast_qtype([arg_1], result),
          )
