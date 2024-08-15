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

"""Tests for M.core.get_second."""

from absl.testing import absltest
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


class CoreGetSecondTest(backend_test_base.SelfEvalMixin):

  def testTuple(self):
    actual_value = self.eval(M.core.get_second(arolla.tuple(0.0, 1, b'2')))
    self.assertEqual(actual_value, arolla.int32(1))

  def testOptional(self):
    actual_value = self.eval(M.core.get_second(arolla.optional_int64(57)))
    self.assertEqual(actual_value, arolla.int64(57))


if __name__ == '__main__':
  absltest.main()
