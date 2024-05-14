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

"""Smoke tests for arolla.arolla."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla


class ArollaTest(parameterized.TestCase):

  def test_smoke(self):
    l = arolla.L
    v = arolla.eval((l.x + l.y) / l.z, x=1.0, y=2.0, z=3.0)
    self.assertEqual(v, 1.0)

  def test_literal(self):
    self.assertEqual(
        arolla.literal(1).fingerprint,
        arolla.literal(arolla.as_qvalue(1)).fingerprint,
    )


if __name__ == '__main__':
  absltest.main()
