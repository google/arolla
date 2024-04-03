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

"""Tests for arolla.abc.utils."""

import functools
import inspect
import sys

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import clib
from arolla.abc import qtype as abc_qtype
from arolla.abc import testing_clib
from arolla.abc import utils as abc_utils

from pybind11_abseil import status as absl_status


class UtilsTest(parameterized.TestCase):

  def test_get_type_name(self):
    self.assertEqual(abc_utils.get_type_name(int), 'int')
    self.assertEqual(abc_utils.get_type_name(str), 'str')
    self.assertEqual(
        abc_utils.get_type_name(abc_qtype.QType), 'arolla.abc.qtype.QType'
    )
    self.assertEqual(
        abc_utils.get_type_name(inspect.Signature), 'inspect.Signature'
    )

  def test_clear_cache(self):
    @functools.lru_cache
    def fn(x):
      return x

    abc_utils.cache_clear_callbacks.add(fn.cache_clear)
    fn(1)
    self.assertEqual(fn.cache_info().currsize, 1)
    abc_utils.clear_caches()
    self.assertEqual(fn.cache_info().currsize, 0)

  def test_numpy_import(self):
    self.assertNotIn('numpy', sys.modules)
    dummy_np = abc_utils.get_numpy_module_or_dummy()
    self.assertNotIn('numpy', sys.modules)
    np = abc_utils.import_numpy()
    self.assertIn('numpy', sys.modules)
    self.assertIsNot(dummy_np, np)
    self.assertIs(np, sys.modules['numpy'])


class ProcessNotOkStatusTest(parameterized.TestCase):

  def test_pure_status(self):
    status = absl_status.Status(
        absl_status.StatusCode.ABORTED, 'This is Status error.'
    )
    status = absl_status.StatusNotOk(status)
    with self.assertRaisesRegex(ValueError, 'This is Status error.'):
      abc_utils.process_status_not_ok(status)

  def test_suppress_context(self):
    raised_exception = None
    try:
      try:
        raise absl_status.StatusNotOk(
            absl_status.Status(absl_status.StatusCode.INTERNAL, 'Some error')
        )
      except absl_status.StatusNotOk as status:
        abc_utils.process_status_not_ok(status)
    except ValueError as ex:
      raised_exception = ex
    self.assertIsNotNone(raised_exception)
    self.assertIsNone(raised_exception.__context__)

  def test_suppress_context_chained(self):
    context_exception = ValueError('Some context')
    raised_exception = None
    try:
      try:
        raise context_exception
      except ValueError:
        try:
          raise absl_status.StatusNotOk(  # pylint: disable=raise-missing-from
              absl_status.Status(absl_status.StatusCode.INTERNAL, 'Some error')
          )
        except absl_status.StatusNotOk as status:
          abc_utils.process_status_not_ok(status)
    except ValueError as ex:
      raised_exception = ex
    self.assertIsNotNone(raised_exception)
    self.assertIs(raised_exception.__context__, context_exception)

  def test_with_py_exception(self):
    status = absl_status.Status(
        absl_status.StatusCode.ABORTED, 'This is Status error.'
    )
    cause = AssertionError('This is the exception.')
    testing_clib.set_py_exception_payload(status, cause)
    status = absl_status.StatusNotOk(status)
    with self.assertRaisesRegex(AssertionError, 'This is the exception.'):
      abc_utils.process_status_not_ok(status)

  def test_with_py_exception_cause(self):
    status = absl_status.Status(
        absl_status.StatusCode.ABORTED, 'This is Status error.'
    )
    cause = AssertionError('This is the cause.')
    testing_clib.set_py_exception_cause_payload(status, cause)
    status = absl_status.StatusNotOk(status)
    raised_exception = None
    try:
      abc_utils.process_status_not_ok(status)
    except ValueError as e:
      raised_exception = e
    self.assertIsNotNone(raised_exception)
    self.assertIsNotNone(raised_exception.__cause__)
    self.assertIs(raised_exception.__cause__, cause)

  def test_ok_status(self):
    with self.assertRaisesRegex(TypeError, 'status must be StatusNotOk'):
      abc_utils.process_status_not_ok(absl_status.Status.OkStatus())  # pytype: disable=wrong-arg-types
    # Extra check for the pure clib wrapper to not crash with ok status.
    clib.raise_if_error(absl_status.Status.OkStatus())


if __name__ == '__main__':
  absltest.main()
