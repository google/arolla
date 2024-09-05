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
from arolla import arolla
from arolla.experimental import reload


class ReloadTest(absltest.TestCase):

  # Check that class ids change after reloading.
  def testChangeAfterReload(self):
    names = [
        'QTYPE',
        'OPERATOR',
        'LeafContainer',
        'LambdaOperator',
    ]
    olds = [getattr(arolla, name) for name in names]
    reload.reload()
    news = [getattr(arolla, name) for name in names]
    for old, new in zip(olds, news):
      self.assertIsNot(old, new)

  # Check that evaluation algorithm is still operating after a reloading.
  def testEvalAfterReload(self):
    reload.reload()
    l = arolla.L
    v = arolla.eval(
        (l.x + l.y) / l.z,
        x=arolla.as_qvalue(1.0),
        y=arolla.as_qvalue(2.0),
        z=arolla.as_qvalue(3.0),
    )
    self.assertEqual(v, 1.0)


if __name__ == '__main__':
  absltest.main()
