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
from arolla.derived_qtype import derived_qtype

L = arolla.L
M = arolla.M | derived_qtype.M


def get_labeled_qtype(qtype, label):
  result = M.derived_qtype.get_labeled_qtype(qtype, label).qvalue
  return result


class DerivedQTypeGetLabeledQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.derived_qtype.get_qtype_label, [(arolla.QTYPE, arolla.TEXT)]
    )

  @parameterized.parameters(
      (arolla.OPTIONAL_INT32, ''),
      (get_labeled_qtype(arolla.OPTIONAL_INT32, 'foo'), 'foo'),
      (get_labeled_qtype(arolla.OPTIONAL_INT32, 'bar'), 'bar'),
  )
  def test_eval(self, qtype, expected_label):
    label = arolla.eval(M.derived_qtype.get_qtype_label(L.qtype), qtype=qtype)
    self.assertIsInstance(label, arolla.types.Text)
    self.assertEqual(label, expected_label)

  @parameterized.parameters(
      (arolla.OPTIONAL_INT32, ''),
      (get_labeled_qtype(arolla.OPTIONAL_INT32, 'foo'), 'foo'),
      (get_labeled_qtype(arolla.OPTIONAL_INT32, 'bar'), 'bar'),
  )
  def test_infer_attr(self, qtype, expected_label):
    label = M.derived_qtype.get_qtype_label(qtype).qvalue
    self.assertIsInstance(label, arolla.types.Text)
    self.assertEqual(label, expected_label)


if __name__ == '__main__':
  absltest.main()
