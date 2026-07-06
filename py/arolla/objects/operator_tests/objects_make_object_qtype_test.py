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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.objects import objects

M = arolla.M | objects.M


class ObjectsMakeObjectQType(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.objects.make_object_qtype, ((arolla.QTYPE,),)
    )

  def test_eval(self):
    result = arolla.eval(M.objects.make_object_qtype())
    self.assertEqual(result.qtype, arolla.QTYPE)
    self.assertNotEqual(result, arolla.NOTHING)
    self.assertEqual(repr(result), 'OBJECT')  # Proxy to test the type.
    self.assertEqual(result, objects.OBJECT)


if __name__ == '__main__':
  absltest.main()
