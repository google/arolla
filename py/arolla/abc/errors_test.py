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
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.abc import testing_clib


class ErrorsTest(parameterized.TestCase):

  def test_verbose_runtime_error(self):
    with self.assertRaises(arolla_abc.VerboseRuntimeError) as cm:
      # See implementation in py/arolla/abc/testing_clib.cc.
      testing_clib.raise_verbose_runtime_error()
    self.assertRegex(
        str(cm.exception), r'\[FAILED_PRECONDITION\] expr evaluation failed'
    )
    self.assertEqual(cm.exception.operator_name, 'test.fail')
    self.assertRegex(str(cm.exception.__cause__), 'error cause')


if __name__ == '__main__':
  absltest.main()
