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

"""Tests for core.with_assertion."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M
POSSIBLE_QTYPES = set(arolla.testing.DETECT_SIGNATURES_DEFAULT_QTYPES) - {
    arolla.NOTHING
}


def gen_qtype_signatures():
  """Yields qtype signatures for core.with_assertion.

  Yields: (value_qtype, condition_qtype, message_qtype, result_qtype)
  """
  for value_qtype in POSSIBLE_QTYPES:
    for condition_qtype in [arolla.UNIT, arolla.OPTIONAL_UNIT]:
      yield (value_qtype, condition_qtype, arolla.TEXT, value_qtype)


QTYPE_SIGNATURES = frozenset(gen_qtype_signatures())


class CoreWithAssertionTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            M.core.with_assertion, possible_qtypes=POSSIBLE_QTYPES
        ),
        QTYPE_SIGNATURES,
    )

  @parameterized.product(
      value=[arolla.int64(1), arolla.array([1, 2, 3])],
      condition=[arolla.present(), arolla.unit()],
  )
  def test_value(self, value, condition):
    actual_value = self.eval(
        M.core.with_assertion(value, condition, 'custom message')
    )
    arolla.testing.assert_qvalue_allequal(value, actual_value)

  def test_error(self):
    # Note: we use a generic exception since the exception type may differ
    # between backends.
    with self.assertRaisesRegex(Exception, 'custom message'):
      _ = self.eval(
          M.core.with_assertion(
              arolla.dense_array([1, 2, 3]),
              arolla.optional_unit(None),
              'custom message',
          )
      )

  def test_arolla_eval_raises_value_error(self):
    # In arolla.eval, the exception type should be a ValueError.
    self.require_self_eval_is_called = False
    with self.assertRaises(ValueError):
      _ = arolla.eval(
          M.core.with_assertion(
              arolla.dense_array([1, 2, 3]),
              arolla.optional_unit(None),
              'custom message',
          )
      )


if __name__ == '__main__':
  absltest.main()
