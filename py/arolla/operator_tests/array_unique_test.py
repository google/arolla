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

"""Tests for M.array.unique."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


def gen_test_cases():
  for array_fn in (arolla.array, arolla.dense_array):
    for tpe in arolla.types.NUMERIC_QTYPES + (arolla.types.UINT64,):
      yield (array_fn([1, 1, 2], tpe), array_fn([1, 2], tpe))
      yield (array_fn([1, None], tpe), array_fn([1], tpe))
      yield (array_fn([1, 2, 1, None]), array_fn([1, 2]))
    yield (array_fn([True, None, False, True, False]), array_fn([True, False]))
    yield (
        array_fn([arolla.unit(), None, arolla.unit()]),
        array_fn([arolla.unit()]),
    )
    yield (array_fn(['1', '1', None]), array_fn(['1']))
    yield (array_fn([b'1', b'1', None]), array_fn([b'1']))

    yield (array_fn([], arolla.FLOAT32), array_fn([], arolla.FLOAT32))
    yield (array_fn([None], arolla.FLOAT32), array_fn([], arolla.FLOAT32))


TEST_CASES = tuple(gen_test_cases())
QTYPE_SIGNATURES = frozenset(
    (arg.qtype, res.qtype) for (arg, res) in TEST_CASES
)


class ArrayUniqueTest(parameterized.TestCase, backend_test_base.SelfEvalMixin):

  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.unique, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_CASES)
  def testValues(self, arg, expected):
    actual = self.eval(M.array.unique(arg))
    arolla.testing.assert_qvalue_allequal(actual, expected)


if __name__ == '__main__':
  absltest.main()
