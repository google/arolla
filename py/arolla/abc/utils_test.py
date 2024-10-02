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
import re
import sys

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import qtype as abc_qtype
from arolla.abc import utils as abc_utils


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

  def test_vectorcall(self):
    def fn(*args, **kwargs):
      return args, kwargs

    self.assertEqual(
        abc_utils.vectorcall(fn, ()),
        ((), {}),
    )
    self.assertEqual(
        abc_utils.vectorcall(fn, 1, 2, 3, ('x',)),
        ((1, 2), dict(x=3)),
    )

    with self.assertRaisesWithLiteralMatch(
        TypeError, "'object' object is not callable"
    ):
      abc_utils.vectorcall(object(), ())  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        TypeError, re.escape('keywords must be strings')
    ):
      abc_utils.vectorcall(fn, 1, 2, (1, 2))
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'too few positional arguments (=4) for the given number of keyword'
            ' names (=5)'
        ),
    ):
      abc_utils.vectorcall(fn, 1, 2, ('a', 'b', 'c', 'd', 'e'))


if __name__ == '__main__':
  absltest.main()
