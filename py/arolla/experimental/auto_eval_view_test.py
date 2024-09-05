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

"""Tests for auto_eval_view."""

from unittest import mock

from absl.testing import absltest
from arolla import arolla
from arolla.experimental import auto_eval_view


class AutoEvalViewTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    auto_eval_view.disable()

  def test_disabled(self):
    auto_eval_view.disable()
    self.assertFalse(hasattr(arolla.M.math.add(100, 5), '_ipython_display_'))

  @mock.patch.object(auto_eval_view, '_display_eval_result')
  def test_enabled(self, mock_display_eval_result):
    auto_eval_view.enable()
    add_expr = arolla.M.math.add(100, 5)
    add_expr._ipython_display_()
    mock_display_eval_result.assert_called_once_with(105)

  @mock.patch.object(auto_eval_view, '_display_eval_result')
  def test_expr_contains_leaf(self, mock_display_eval_result):
    auto_eval_view.enable()
    add_expr = arolla.M.math.add(100, arolla.L.x)
    add_expr._ipython_display_()
    mock_display_eval_result.assert_not_called()

  @mock.patch.object(auto_eval_view, '_display_eval_result')
  def test_expr_contains_placeholder(self, mock_display_eval_result):
    auto_eval_view.enable()
    add_expr = arolla.M.math.add(100, arolla.P.x)
    add_expr._ipython_display_()
    mock_display_eval_result.assert_not_called()


if __name__ == '__main__':
  absltest.main()
