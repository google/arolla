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

"""Tests for M.strings.join_with_separator operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils


M = arolla.M


def gen_qtype_test_signatures():
  for sep_qtype in (arolla.TEXT, arolla.BYTES):
    arg_qtypes = list(pointwise_test_utils.lift_qtypes(sep_qtype))
    for n in (1, 2, 3):
      for args_n_qtypes in itertools.product(arg_qtypes, repeat=n):
        with contextlib.suppress(arolla.types.QTypeError):
          yield (
              sep_qtype,
              *args_n_qtypes,
              arolla.types.common_qtype(*args_n_qtypes),
          )


def gen_test_data(sample_tokens):
  for sep in sample_tokens:
    for n in (1, 2, 3):
      for args_n in itertools.product(sample_tokens + [None], repeat=n):
        if None in args_n:
          yield (sep, *args_n, None)
        else:
          yield (sep, *args_n, sep.join(args_n))


QTYPE_SIGNATURES = tuple(gen_qtype_test_signatures())
TEST_DATA = (
    *gen_test_data(['', 'Hello', 'World']),
    *gen_test_data([b'', b'Hello', b'World']),
)


class StringsJoinWithSeparatorTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.strings.join_with_separator, QTYPE_SIGNATURES, max_arity=4
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def test_eval(self, *test_case):
    args = test_case[:-1]
    expected_result = test_case[-1]
    arolla.testing.assert_qvalue_allclose(
        self.eval(M.strings.join_with_separator(*args)), expected_result
    )


if __name__ == '__main__':
  absltest.main()
