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

"""Tests for backend_test_base."""

import re
from unittest import mock

from absl import flags
from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from arolla.operator_tests import backend_test_base


def fake_eval():
  return 'fake_eval'


INT_CONST = 1

# Required flags have to be set before absltest.main(). We set eval_fn to a
# dummy value.
FLAGS = flags.FLAGS
FLAGS.eval_fn = ''


class SelfEvalMixinTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    backend_test_base._get_eval_fn.cache_clear()
    backend_test_base._get_skip_test_pattern.cache_clear()
    self.eval_base = backend_test_base.SelfEvalMixin()

  def test_eval_simple(self):
    eval_path = 'arolla.operator_tests.backend_test_base_test.fake_eval'
    with flagsaver.flagsaver(eval_fn=eval_path):
      self.assertEqual(self.eval_base.eval(), fake_eval())

  def test_eval_fn_path_not_set(self):
    with self.assertRaises(flags.IllegalFlagValueError):
      FLAGS.eval_fn = None

  def test_invalid_path(self):
    invalid_path = 'abc.def.test.fake'
    with flagsaver.flagsaver(eval_fn=invalid_path):
      with self.assertRaisesRegex(ImportError, invalid_path):
        self.eval_base.eval()

  def test_not_a_function(self):
    int_path = 'arolla.operator_tests.backend_test_base_test.INT_CONST'
    with flagsaver.flagsaver(eval_fn=int_path):
      with self.assertRaisesRegex(
          TypeError, f'{int_path!r}.*callable.*was.*int'
      ):
        self.eval_base.eval()

  def test_require_self_eval_is_called(self):
    self.eval_base.setUp()
    self.assertTrue(self.eval_base.require_self_eval_is_called)
    self.eval_base.require_self_eval_is_called = False
    self.assertFalse(self.eval_base.require_self_eval_is_called)
    with self.assertRaisesRegex(
        TypeError, '`require_self_eval_is_called` must be set to a bool'
    ):
      self.eval_base.require_self_eval_is_called = 'abc'

  def test_no_raise_if_eval_is_called(self):
    self.eval_base.setUp()
    eval_path = 'arolla.operator_tests.backend_test_base_test.fake_eval'
    with flagsaver.flagsaver(eval_fn=eval_path):
      _ = self.eval_base.eval()
    self.eval_base.tearDown()

  def test_raise_if_eval_not_called(self):
    self.eval_base.setUp()
    with self.assertRaisesRegex(AssertionError, '`self.eval` was not called'):
      self.eval_base.tearDown()

  def test_no_raise_if_eval_not_required(self):
    self.eval_base.setUp()
    self.eval_base.require_self_eval_is_called = False
    self.eval_base.tearDown()

  @parameterized.parameters(
      (['.*method'], '.*method'),
      (['abc', r'cls\.method'], r'abc|cls\.method'),
      (['abc', r'^cls\.method_123$'], r'abc|^cls\.method_123$'),
  )
  def test_skip_test_when_pattern_matches(self, skip_test_patterns, message):
    with flagsaver.flagsaver(skip_test_patterns=skip_test_patterns):
      with mock.patch.object(
          self.eval_base, 'id', return_value='module.cls.method_123'
      ):
        # Assert that the test is skipped.
        with self.assertRaisesRegex(
            absltest.SkipTest, f'test ID matches {re.escape(message)}'
        ):
          self.eval_base.setUp()

  @parameterized.parameters(
      (['abc', r'cls\.method$'],),
      (['method_123'],),
      (['^method_123'],),
      ([],),
  )
  def test_do_test_when_pattern_does_not_match(self, skip_test_patterns):
    with flagsaver.flagsaver(skip_test_patterns=skip_test_patterns):
      with mock.patch.object(
          self.eval_base, 'id', return_value='module.cls.method_123'
      ):
        try:
          self.eval_base.setUp()
        except absltest.SkipTest:
          self.fail()


if __name__ == '__main__':
  absltest.main()
