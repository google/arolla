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

"""Test for qtype signatures for M.bool.logical_if operator.

Note: We separate type_signatures and value computation test targets for
performance reasons.
"""

from absl.testing import absltest
from arolla import arolla
from arolla.operator_tests import bool_logical_if_test_helper

M = arolla.M


class BoolLogicalIfTest(absltest.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.bool.logical_if,
        bool_logical_if_test_helper.gen_qtype_signatures(),
    )


if __name__ == '__main__':
  absltest.main()
