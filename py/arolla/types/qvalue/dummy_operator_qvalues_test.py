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

"""Tests for dummy_operator_qvalues."""

import inspect

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.s11n import s11n as arolla_s11n
from arolla.types.qtype import array_qtypes
from arolla.types.qtype import scalar_qtypes
from arolla.types.qvalue import dummy_operator_qvalues


class DummyOperatorQvalueTest(parameterized.TestCase):

  def test_name(self):
    op = dummy_operator_qvalues.DummyOperator(
        'test_name', 'x, y, z', result_qtype=array_qtypes.ARRAY_FLOAT32
    )
    self.assertEqual(op.display_name, 'test_name')

  @parameterized.parameters(inspect.signature(lambda x, y, z: None), 'x, y, z')
  def test_signature(self, signature):
    op = dummy_operator_qvalues.DummyOperator(
        'test_name', signature, result_qtype=array_qtypes.ARRAY_FLOAT32
    )
    signature = inspect.signature(op)
    self.assertListEqual(list(signature.parameters.keys()), ['x', 'y', 'z'])

  def test_doc(self):
    op = dummy_operator_qvalues.DummyOperator(
        'test_name',
        'x, y, z',
        doc='op doc',
        result_qtype=array_qtypes.ARRAY_FLOAT32,
    )
    self.assertIn(' DummyOperator', type(op).__doc__)
    self.assertEqual(op.getdoc(), 'op doc')

  @parameterized.parameters(
      array_qtypes.ARRAY_FLOAT32,
      array_qtypes.ARRAY_FLOAT64,
      arolla_abc.NOTHING,
  )
  def test_qtype(self, qtype):
    op = dummy_operator_qvalues.DummyOperator(
        'test_name', 'x', result_qtype=qtype
    )
    node = op(scalar_qtypes.float32(1.0))
    self.assertEqual(node.qtype, qtype)

  def test_serialization_round_trip(self):
    op = dummy_operator_qvalues.DummyOperator(
        'test_name',
        'x, y, z',
        doc='op doc',
        result_qtype=array_qtypes.ARRAY_FLOAT32,
    )
    self.assertEqual(
        arolla_s11n.loads(arolla_s11n.dumps(op)).fingerprint, op.fingerprint
    )


if __name__ == '__main__':
  absltest.main()
