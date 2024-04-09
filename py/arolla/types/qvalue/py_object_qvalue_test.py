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

"""Tests for arolla.types.qtype.py_object_qvalue."""

import re

from absl.testing import absltest
from arolla.abc import abc as rl_abc
from arolla.types.qtype import py_object_qtype as rl_py_object_qtype
from arolla.types.qvalue import py_object_qvalue as rl_py_object_qvalue


class PyObjectQValueTest(absltest.TestCase):

  def testBoxing(self):
    x = rl_py_object_qvalue.PyObject(object())
    self.assertIsInstance(x, rl_abc.QValue)
    self.assertIsInstance(x, rl_py_object_qvalue.PyObject)
    self.assertEqual(x.qtype, rl_py_object_qtype.PY_OBJECT)

  def testBoxingOfQValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a python type, got a natively supported QTYPE'),
    ):
      _ = rl_py_object_qvalue.PyObject(rl_abc.NOTHING)

  def testUnboxing(self):
    obj = object()
    self.assertIs(rl_py_object_qvalue.PyObject(obj).py_value(), obj)

  def testCodec(self):
    x = rl_py_object_qvalue.PyObject(object(), b'test')
    self.assertEqual(x.codec(), b'test')

  def testCodecNone(self):
    x = rl_py_object_qvalue.PyObject(object())
    self.assertIsNone(x.codec())


if __name__ == '__main__':
  absltest.main()
