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

"""Tests for complex."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.examples.my_complex import my_complex


class MyComplexTest(parameterized.TestCase):

  def test_qtype(self):
    self.assertIsInstance(my_complex.MY_COMPLEX, arolla.QType)
    self.assertEqual(my_complex.MY_COMPLEX.name, 'MY_COMPLEX')

  def test_new(self):
    c = my_complex.MyComplex(5.7, 0.7)
    self.assertEqual(c.re, 5.7)
    self.assertEqual(c.im, 0.7)

  def test_repr(self):
    c = my_complex.MyComplex(5.7, 0.7)
    self.assertEqual(repr(c), '5.7 + 0.7i')


if __name__ == '__main__':
  absltest.main()
