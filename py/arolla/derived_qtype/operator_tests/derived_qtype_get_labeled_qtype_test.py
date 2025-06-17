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


class DerivedQTypeGetLabeledQTypeTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        M.derived_qtype.get_labeled_qtype,
        [(arolla.QTYPE, arolla.TEXT, arolla.QTYPE)],
    )

  def test_eval(self):
    labeled_qtype = arolla.eval(
        M.derived_qtype.get_labeled_qtype(L.qtype, L.label),
        qtype=arolla.OPTIONAL_INT32,
        label='foo',
    )
    self.assertIsInstance(labeled_qtype, arolla.QType)
    self.assertEqual(labeled_qtype.name, 'LABEL[foo]')
    self.assertIsNone(labeled_qtype.value_qtype)
    self.assertEqual(
        arolla.eval(M.derived_qtype.get_labeled_qtype(labeled_qtype, '')),
        arolla.OPTIONAL_INT32,
    )

  def test_infer_attr(self):
    labeled_qtype = M.derived_qtype.get_labeled_qtype(
        arolla.OPTIONAL_INT32, 'bar'
    ).qvalue
    self.assertIsInstance(labeled_qtype, arolla.QType)
    self.assertEqual(labeled_qtype.name, 'LABEL[bar]')
    self.assertIsNone(labeled_qtype.value_qtype)
    self.assertEqual(
        M.derived_qtype.get_labeled_qtype(labeled_qtype, '').qvalue,
        arolla.OPTIONAL_INT32,
    )

  def test_relabel(self):
    relabeled_qtype = arolla.eval(
        M.derived_qtype.get_labeled_qtype(
            M.derived_qtype.get_labeled_qtype(L.qtype, L.label_1),
            L.label_2,
        ),
        qtype=arolla.OPTIONAL_INT32,
        label_1='foo',
        label_2='bar',
    )
    self.assertEqual(relabeled_qtype.name, 'LABEL[bar]')
    self.assertIsNone(relabeled_qtype.value_qtype)
    self.assertEqual(
        M.derived_qtype.get_labeled_qtype(relabeled_qtype, '').qvalue,
        arolla.OPTIONAL_INT32,
    )


if __name__ == '__main__':
  absltest.main()
