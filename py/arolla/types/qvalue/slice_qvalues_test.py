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

"""Tests for slice_qvalue."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.operators import operators_clib as _
from arolla.types.qtype import scalar_qtypes
from arolla.types.qvalue import scalar_qvalues as _
from arolla.types.qvalue import slice_qvalues


class SliceQvalueTest(parameterized.TestCase):

  def testType(self):
    qvalue = arolla_abc.invoke_op(
        'core.make_slice',
        (
            arolla_abc.unspecified(),
            arolla_abc.unspecified(),
            arolla_abc.unspecified(),
        ),
    )
    self.assertIsInstance(qvalue, slice_qvalues.Slice)
    self.assertEqual(qvalue.fingerprint, slice_qvalues.Slice().fingerprint)

  def testStartStopStep(self):
    start = scalar_qtypes.int32(0)
    stop = scalar_qtypes.float32(1)
    step = arolla_abc.unspecified()
    qvalue = slice_qvalues.Slice(start, stop, step)
    self.assertEqual(qvalue.start.fingerprint, start.fingerprint)
    self.assertEqual(qvalue.stop.fingerprint, stop.fingerprint)
    self.assertEqual(qvalue.step.fingerprint, step.fingerprint)

  def testAutoBoxing(self):
    qvalue = slice_qvalues.Slice(1, 2.0, True)
    self.assertEqual(
        qvalue.start.fingerprint, scalar_qtypes.int32(1).fingerprint
    )
    self.assertEqual(
        qvalue.stop.fingerprint, scalar_qtypes.float32(2.0).fingerprint
    )
    self.assertEqual(
        qvalue.step.fingerprint, scalar_qtypes.boolean(True).fingerprint
    )

  def testDefaults(self):
    qvalue = slice_qvalues.Slice()
    self.assertEqual(
        qvalue.start.fingerprint, arolla_abc.unspecified().fingerprint
    )
    self.assertEqual(
        qvalue.stop.fingerprint, arolla_abc.unspecified().fingerprint
    )
    self.assertEqual(
        qvalue.step.fingerprint, arolla_abc.unspecified().fingerprint
    )

  def testPyValue(self):
    qvalue = slice_qvalues.Slice(0, 1.0, arolla_abc.unspecified())
    self.assertEqual(qvalue.py_value(), slice(0, 1.0, None))

  def testRepr(self):
    self.assertEqual(
        repr(slice_qvalues.Slice(0, 1.0, arolla_abc.unspecified())),
        'slice(0, 1., unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
