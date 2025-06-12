# Copyright 2025 Google LLC
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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as _  # trigger InitArolla()
from arolla.abc import testing_clib


class ErrorConvertersTest(parameterized.TestCase):

  def test_verbose_runtime_error(self):
    with self.assertRaisesRegex(ValueError, 'error cause') as cm:
      # See implementation in py/arolla/abc/testing_clib.cc.
      testing_clib.raise_verbose_runtime_error()
    self.assertEqual(getattr(cm.exception, 'operator_name'), 'test.fail')
    self.assertEqual(
        getattr(cm.exception, '__notes__'), ['operator_name: test.fail']
    )

  def test_invalid_verbose_runtime_error(self):
    with self.assertRaisesRegex(
        AssertionError,
        re.escape(
            "invalid VerboseRuntimeError(status.code=9, status.message='expr"
            " evaluation\\nfailed', operator_name='test.fail')"
        ),
    ):
      # See implementation in py/arolla/abc/testing_clib.cc.
      testing_clib.raise_invalid_verbose_runtime_error()

  def test_error_with_note(self):
    with self.assertRaisesRegex(
        ValueError, '\\[FAILED_PRECONDITION\\] original error'
    ) as cm:
      # See implementation in py/arolla/abc/testing_clib.cc.
      testing_clib.raise_error_with_note()
    self.assertEqual(getattr(cm.exception, '__notes__'), ['Added note'])

  def test_error_with_two_notes(self):
    with self.assertRaisesRegex(
        ValueError, '\\[FAILED_PRECONDITION\\] original error'
    ) as cm:
      # See implementation in py/arolla/abc/testing_clib.cc.
      testing_clib.raise_error_with_two_notes()
    self.assertEqual(
        getattr(cm.exception, '__notes__'), ['Added note', 'Another added note']
    )

  def test_invalid_error_with_note(self):
    with self.assertRaisesRegex(
        AssertionError,
        re.escape(
            "invalid NotePayload(status.code=9, status.message='original"
            "\\nerror', note='Added note')"
        ),
    ):
      # See implementation in py/arolla/abc/testing_clib.cc.
      testing_clib.raise_invalid_error_with_note()


if __name__ == '__main__':
  absltest.main()
