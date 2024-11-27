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

from absl.testing import absltest
from arolla.abc import abc as arolla_abc
from arolla.optools import decorators
from arolla.optools import helpers
from arolla.testing import testing as arolla_testing
from arolla.types import types as arolla_types


class MakeLambdaTest(absltest.TestCase):

  def test_lambda(self):
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        helpers.make_lambda(arolla_abc.placeholder('x'), name='foo', doc='bar'),
        arolla_types.LambdaOperator(
            arolla_abc.placeholder('x'), name='foo', doc='bar'
        ),
    )

  def test_restricted_lambda(self):
    arolla_testing.assert_qvalue_equal_by_fingerprint(
        helpers.make_lambda(
            arolla_abc.placeholder('x'),
            name='foo',
            doc='bar',
            qtype_constraints=[(arolla_types.present(), 'errro-message')],
        ),
        arolla_types.RestrictedLambdaOperator(
            arolla_abc.placeholder('x'),
            name='foo',
            doc='bar',
            qtype_constraints=[(arolla_types.present(), 'errro-message')],
        ),
    )


class SuppressUnusedParameterWarningTest(absltest.TestCase):
  """A smoke tests for arolla.optools.suppress_unused_parameter_warning."""

  def test_returns_first_arg(self):
    self.assertEqual(
        arolla_abc.eval_expr(
            helpers.suppress_unused_parameter_warning(arolla_abc.QTYPE, 2, 3, 4)
        ),
        arolla_abc.QTYPE,
    )

  def test_warning(self):
    # Expect a warning in the log (we cannot enforce in the unit-test, though).

    @decorators.as_lambda_operator('op_with_warning')
    def op_with_warning(
        arg1, expected_unused_warning_arg2, expected_unused_warning_arg3
    ):
      del expected_unused_warning_arg2, expected_unused_warning_arg3
      return arg1

    del op_with_warning

  def test_no_warning(self):
    # Expect a warning in the log (we cannot enforce in the unit-test, though).

    @decorators.as_lambda_operator('op_with_warning')
    def op_with_no_warning(arg1, arg2, arg3):
      return helpers.suppress_unused_parameter_warning(arg1, arg2, arg3)

    del op_with_no_warning


if __name__ == '__main__':
  absltest.main()
