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

"""A smoke tests for arolla.optools.suppress_unused_parameter_warning."""

from absl.testing import absltest
from arolla.abc import abc as arolla_abc
from arolla.optools import optools as arolla_optools


class SuppressUnusedParameterWarningTest(absltest.TestCase):

  def test_returns_first_arg(self):
    self.assertEqual(
        arolla_abc.eval_expr(
            arolla_optools.suppress_unused_parameter_warning(
                arolla_abc.QTYPE, 2, 3, 4
            )
        ),
        arolla_abc.QTYPE,
    )

  def test_warning(self):
    # Expect a warning in the log (we cannot enforce in the unit-test, though).

    @arolla_optools.as_lambda_operator('op_with_warning')
    def op_with_warning(arg1, arg2, expected_unused_warning_arg3):
      del expected_unused_warning_arg3
      return arg1 | arg2

    del op_with_warning

  def test_no_warning(self):
    # Expect a warning in the log (we cannot enforce in the unit-test, though).

    @arolla_optools.as_lambda_operator('op_with_warning')
    def op_with_no_warning(arg1, arg2, arg3):
      return arolla_optools.suppress_unused_parameter_warning(arg1 | arg2, arg3)

    del op_with_no_warning


if __name__ == '__main__':
  absltest.main()
