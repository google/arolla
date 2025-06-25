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

"""Tests for M.annotation.source_location operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M
L = arolla.L
P = arolla.P


class AnnotationSourceLocationTest(parameterized.TestCase):

  def testOk(self):
    x1 = M.annotation.source_location(
        L.x, 'func', 'file.py', 1, 2, 'x = y + 1'
    )  # no except
    x2 = M.annotation.source_location(
        L.x,
        function_name='func',
        file_name='file.py',
        line=1,
        column=2,
        line_text='x = y + 1',
    )  # no except
    arolla.testing.assert_expr_equal_by_fingerprint(x1, x2)

  def testErrorNonLiteral(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`function_name` must be a TEXT literal')
    ):
      M.annotation.source_location(L.x, P.x, 'file.py', 1, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError, re.escape('`file_name` must be a TEXT literal')
    ):
      M.annotation.source_location(L.x, 'func', P.x, 1, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError, re.escape('`line` must be a INT32 literal')
    ):
      M.annotation.source_location(L.x, 'func', 'file.py', P.x, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError, re.escape('`column` must be a INT32 literal')
    ):
      M.annotation.source_location(L.x, 'func', 'file.py', 1, P.x, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError, re.escape('`line_text` must be a TEXT literal')
    ):
      M.annotation.source_location(L.x, 'func', 'file.py', 1, 2, P.x)

  def testErrorWrongType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a TEXT literal, got function_name: BYTES'),
    ):
      M.annotation.source_location(L.x, b'func', 'file.py', 1, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a TEXT literal, got file_name: BYTES'),
    ):
      M.annotation.source_location(L.x, 'func', b'file.py', 1, 2, 'x = y + 1')

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a INT32 literal, got line: INT64'),
    ):
      M.annotation.source_location(
          L.x, 'func', 'file.py', arolla.int64(1), 2, 'x = y + 1'
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a INT32 literal, got column: INT64'),
    ):
      M.annotation.source_location(
          L.x, 'func', 'file.py', 1, arolla.int64(2), 'x = y + 1'
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a TEXT literal, got line_text: BYTES'),
    ):
      M.annotation.source_location(
          L.x, 'func', 'file.py', 1, 2, b'x = y + 1'
      )


if __name__ == '__main__':
  absltest.main()
