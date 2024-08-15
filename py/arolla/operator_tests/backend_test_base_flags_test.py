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

"""Tests for backend_test_base_flags."""

from absl.testing import absltest
from absl.testing import flagsaver
from absl.testing import parameterized
from arolla.operator_tests import backend_test_base_flags


class BackendTestBaseFlagsTest(parameterized.TestCase):

  def test_default_empty(self):
    self.assertEmpty(backend_test_base_flags.get_extra_flag('foo'))

  def test_values(self):
    with flagsaver.flagsaver(
        extra_flags=['foo:bar:baz', 'foo:bar', 'bar:bq', 'foo:baz']
    ):
      self.assertListEqual(
          backend_test_base_flags.get_extra_flag('foo'),
          ['bar:baz', 'bar', 'baz'],
      )
      self.assertListEqual(
          backend_test_base_flags.get_extra_flag('bar'), ['bq']
      )

  def test_invalid_flag(self):
    with flagsaver.flagsaver(extra_flags=['foo']):
      with self.assertRaisesWithLiteralMatch(
          ValueError,
          "invalid `extra_flags` value: 'foo'. Expected the form"
          ' `<flag_name>:<contents>`',
      ):
        backend_test_base_flags.get_extra_flag('foo')


if __name__ == '__main__':
  absltest.main()
