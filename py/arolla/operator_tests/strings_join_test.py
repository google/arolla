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

"""Tests for M.strings.join operator."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils


M = arolla.M


def gen_test_data():
  """Yields test data for strings.join operator.

  Yields: (*args, result)
  """
  text_values = ('abc', 'def', '')
  bytes_values = (b'abc', b'def', b'')
  for values in (text_values, bytes_values):
    sep = '' if isinstance(values[0], str) else b''
    for repeat in (1, 2, 3):
      for args in itertools.product(values, repeat=repeat):
        yield *args, sep.join(args)


def gen_qtype_test_signatures():
  """Yields qtype signatures for strings.join with arity in [1, 3].

  Yields: (*arg_qtype, result_qtype)
  """
  qtypes = pointwise_test_utils.lift_qtypes(arolla.TEXT, arolla.BYTES)
  for repeat in (1, 2, 3):
    for args in itertools.product(qtypes, repeat=repeat):
      with contextlib.suppress(arolla.types.QTypeError):
        yield *args, arolla.types.common_qtype(*args)


TEST_CASES = tuple(
    (f'arity={len(tc)-1}', *tc)
    for tc in pointwise_test_utils.gen_cases(
        tuple(gen_test_data()), *gen_qtype_test_signatures()
    )
)


class StringsJoinTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  @parameterized.parameters(*TEST_CASES)
  def testValue(self, unused_arity, *args_and_result):
    args, expected_result = args_and_result[:-1], args_and_result[-1]
    result = self.eval(M.strings.join(*args))
    arolla.testing.assert_qvalue_allequal(result, expected_result)

  def testAtLeastOneArgumentError(self):
    self.require_self_eval_is_called = False
    with self.assertRaises(TypeError):
      _ = M.strings.join()


if __name__ == '__main__':
  absltest.main()
