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

"""Tests for arolla.lazy."""

import re
from absl.testing import absltest
from arolla import arolla
from arolla.lazy import lazy

M = arolla.OperatorsContainer(lazy)
L = arolla.L


class LazyTest(absltest.TestCase):

  def test_lazy_from_callable_without_qtype(self):
    x = lazy.Lazy.from_callable(lambda: 1)
    self.assertIsInstance(x, arolla.QValue)
    self.assertIsInstance(x, lazy.Lazy)
    self.assertEqual(repr(x), 'lazy[PY_OBJECT]')
    self.assertEqual(repr(x.qtype), 'LAZY[PY_OBJECT]')
    self.assertIsInstance(x.get(), arolla.types.PyObject)
    self.assertEqual(x.get().py_value(), 1)

  def test_lazy_from_callable_with_qtype(self):
    x = lazy.Lazy.from_callable(lambda: arolla.int32(1), qtype=arolla.INT32)
    self.assertIsInstance(x, arolla.QValue)
    self.assertIsInstance(x, lazy.Lazy)
    self.assertEqual(repr(x), 'lazy[INT32]')
    self.assertEqual(repr(x.qtype), 'LAZY[INT32]')
    arolla.testing.assert_qvalue_allequal(x.get(), arolla.int32(1))

  def test_lazy_from_callable_with_explicit_py_object_qtype(self):
    x = lazy.Lazy.from_callable(
        lambda: arolla.types.PyObject(1), qtype=arolla.types.PY_OBJECT
    )
    self.assertIsInstance(x, arolla.QValue)
    self.assertIsInstance(x, lazy.Lazy)
    self.assertEqual(repr(x), 'lazy[PY_OBJECT]')
    self.assertEqual(repr(x.qtype), 'LAZY[PY_OBJECT]')
    self.assertIsInstance(x.get(), arolla.types.PyObject)
    self.assertEqual(x.get().py_value(), 1)

  def test_callable_raises_error(self):
    def fn():
      raise RuntimeError('fail')

    e = None
    try:
      lazy.Lazy.from_callable(fn).get()
      self.fail('expected a ValueError')
    except ValueError as ex:
      e = ex

    self.assertRegex(
        str(e), re.escape('[FAILED_PRECONDITION] a lazy callable has failed')
    )
    self.assertIsInstance(e.__cause__, RuntimeError)
    self.assertEqual(str(e.__cause__), 'fail')

  def test_type_error(self):
    e = None
    try:
      lazy.Lazy.from_callable(object, qtype=arolla.INT32).get()
      self.fail('expected a ValueError')
    except ValueError as ex:
      e = ex
    self.assertRegex(
        str(e),
        re.escape(
            '[FAILED_PRECONDITION] a lazy callable returned unexpected python'
            ' type'
        ),
    )
    self.assertIsInstance(e.__cause__, TypeError)
    self.assertEqual(str(e.__cause__), 'expected QValue, got object')

  def test_qtype_mismatch(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            '[FAILED_PRECONDITION] expected a lazy callable to return INT32,'
            ' got FLOAT32'
        ),
    ):
      lazy.Lazy.from_callable(
          lambda: arolla.float32(1), qtype=arolla.INT32
      ).get()

  def test_operator_container(self):
    self.assertEqual(
        arolla.eval(
            M.lazy.get(L.x), x=lazy.Lazy.from_callable(lambda: 1)
        ).py_value(),
        1,
    )


if __name__ == '__main__':
  absltest.main()
