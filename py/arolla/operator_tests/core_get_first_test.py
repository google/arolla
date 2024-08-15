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

"""Tests for M.core.get_first."""

from absl.testing import absltest
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


class CoreGetFirstTest(backend_test_base.SelfEvalMixin):

  def testTuple(self):
    actual_value = self.eval(M.core.get_first(arolla.tuple(0.0, 1, b'2')))
    self.assertEqual(actual_value, arolla.float32(0.0))

  def testOptional(self):
    actual_value = self.eval(M.core.get_first(arolla.optional_int64(57)))
    self.assertEqual(actual_value, arolla.boolean(True))


if __name__ == '__main__':
  absltest.main()
