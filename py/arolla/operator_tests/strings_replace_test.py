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

"""Tests for M.strings.replace."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

# Test data: tuple((arg, expected_result), ...)
TEST_DATA = (
    (None, None, None, None, None),
    ('yeah yeah yeah', 'yeah', 'no', None, 'no no no'),
    ('yeah yeah yeah', 'yeah', 'no', -2, 'no no no'),
    ('yeah yeah yeah', 'yeah', 'no', -1, 'no no no'),
    ('yeah yeah yeah', 'yeah', 'no', 0, 'yeah yeah yeah'),
    ('yeah yeah yeah', 'yeah', 'no', 1, 'no yeah yeah'),
    ('yeah yeah yeah', 'yeah', 'no', 2, 'no no yeah'),
    ('yeah yeah yeah', 'yeah', 'no', 3, 'no no no'),
    ('yes', '', '-', None, '-y-e-s-'),
    ('yes', '', '-', -1, '-y-e-s-'),
    ('yes', '', '-', 0, 'yes'),
    ('yes', '', '-', 1, '-yes'),
    ('yes', '', '-', 2, '-y-es'),
    ('yes', '', '-', 3, '-y-e-s'),
    ('Yeah yeah YEAH', 'yeah', 'No', None, 'Yeah No YEAH'),
)

_encode = lambda x: None if x is None else x.encode('utf8')

TEST_DATA = TEST_DATA + tuple(
    (_encode(x[0]), _encode(x[1]), _encode(x[2]), x[3], _encode(x[4]))
    for x in TEST_DATA
)


def gen_qtype_signatures():
  for string_qtype in (arolla.TEXT, arolla.BYTES):
    lifted_strings = pointwise_test_utils.lift_qtypes(string_qtype)
    for s, old, new, max_subs in itertools.product(
        lifted_strings,
        lifted_strings,
        lifted_strings,
        (arolla.INT32, arolla.OPTIONAL_INT32),
    ):
      with contextlib.suppress(arolla.types.QTypeError):
        yield s, old, new, max_subs, arolla.types.common_qtype(s, old, new)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class StringsReplaceTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False

    @arolla.optools.as_lambda_operator('replace_with_fixed_max_subs')
    def replace_with_fixed_max_subs(s, old, new):
      return M.strings.replace(s, old, new)

    detected_qtypes = set()
    for sig in pointwise_test_utils.detect_qtype_signatures(
        replace_with_fixed_max_subs
    ):
      for max_subs_type in (arolla.INT32, arolla.OPTIONAL_INT32):
        detected_qtypes.add(sig[:3] + (max_subs_type,) + sig[3:])

    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(detected_qtypes),
    )

  @parameterized.parameters(
      pointwise_test_utils.gen_cases(TEST_DATA, *QTYPE_SIGNATURES)
  )
  def testValue(self, arg1, arg2, arg3, arg4, expected_value):
    actual_value = self.eval(M.strings.replace(arg1, arg2, arg3, arg4))
    arolla.testing.assert_qvalue_allequal(actual_value, expected_value)


if __name__ == '__main__':
  absltest.main()
