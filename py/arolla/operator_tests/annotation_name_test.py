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

"""Tests for M.annotation.name."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M
L = arolla.L
P = arolla.P


class AnnotationNameTest(parameterized.TestCase):

  @parameterized.parameters(
      '',
      'name',
  )
  def testOk(self, name):
    _ = M.annotation.name(L.x, name)  # no except

  def testErrorNonLiteral(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('`name` must be a TEXT literal')
    ):
      _ = M.annotation.name(L.x, P.x)

  def testErrorNonText(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a TEXT literal, got name: BYTES')
    ):
      _ = M.annotation.name(L.x, b'baz')


if __name__ == '__main__':
  absltest.main()
