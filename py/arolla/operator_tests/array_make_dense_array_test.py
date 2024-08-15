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

"""Tests for M.array.make_dense_array."""

import contextlib
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils

L = arolla.L
M = arolla.M

# Adding more values is not recommended as it causes `gen_testcases(3)` to
# explode.
TEST_DATA = (None, False, True, b'', b'foo', '', 'bar', 1, float('nan'))


def gen_qtype_signatures(arity):
  """Yields all supported signatures with the given arity."""
  value_qtypes = arolla.types.SCALAR_QTYPES + arolla.types.OPTIONAL_QTYPES
  for xs in itertools.product(value_qtypes, repeat=arity):
    with contextlib.suppress(arolla.types.QTypeError):
      yield (
          *xs,
          arolla.types.make_dense_array_qtype(arolla.types.common_qtype(*xs)),
      )


def gen_testcases(arity):
  test_data = tuple(zip(TEST_DATA))
  for qtype_signature in gen_qtype_signatures(arity):
    arg_values = [
        pointwise_test_utils.gen_cases(test_data, (arg_qtype,))
        for arg_qtype in qtype_signature[:-1]
    ]
    for args in itertools.product(*arg_values):
      # The args are in the form ((arg_0,), (arg_1), ...).
      args = [x[0] for x in args]
      # TODO: arolla.dense_array(uint64s) doesn't work currently.
      py_args = [arg.py_value() for arg in args]
      yield (
          *args,
          arolla.dense_array(py_args, qtype_signature[-1].value_qtype),
      )


def _bind_args_n(op, arity):
  args = [f'x{i}' for i in range(arity)]
  return arolla.LambdaOperator(
      ', '.join(args), op(*[arolla.abc.placeholder(a) for a in args])
  )


class MakeDenseArrayTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  @parameterized.parameters(1, 2, 3)
  def testQTypeSignatures(self, arity):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        _bind_args_n(M.array.make_dense_array, arity),
        gen_qtype_signatures(arity),
    )

  @parameterized.parameters(
      *gen_testcases(1),
      *gen_testcases(2),
      *gen_testcases(3),
  )
  def testResult(self, *args):
    args, expected = args[:-1], args[-1]
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.array.make_dense_array(*args)), expected
    )

  def testErrors(self):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError,
        r'arguments do not have a common type: x0: INT32, '
        r'\*xs: \(BYTES, INT32\)',
    ):
      M.array.make_dense_array(1, b'foo', 3)
    with self.assertRaisesRegex(
        ValueError,
        r'expected all arguments to be scalars or optionals, '
        r'got x0: INT32, \*xs: \(DENSE_ARRAY_INT32\)',
    ):
      M.array.make_dense_array(1, M.array.make_dense_array(1, 2))


if __name__ == '__main__':
  absltest.main()
