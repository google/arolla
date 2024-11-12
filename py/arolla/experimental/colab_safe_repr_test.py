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

import sys
import types
from unittest import mock
from absl.testing import absltest


sentinel_safe_repr = object()
fake_colab_inspector_module = types.SimpleNamespace()
fake_colab_inspector_module._safe_repr = sentinel_safe_repr


class ColabSafeReprTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    with mock.patch.dict(
        sys.modules, {'google.colab._inspector': fake_colab_inspector_module}
    ):
      import arolla.experimental.colab_safe_repr  # pylint: disable=g-import-not-at-top

      self._module = arolla.experimental.colab_safe_repr
      self._module.disable()

  @mock.patch.dict(
      sys.modules, {'google.colab._inspector': fake_colab_inspector_module}
  )
  def test_enable_disable(self):
    self._module.enable()
    self.assertIs(fake_colab_inspector_module._safe_repr, self._module._impl)
    self.assertIs(self._module._original_safe_repr, sentinel_safe_repr)
    self._module.disable()
    self.assertIs(fake_colab_inspector_module._safe_repr, sentinel_safe_repr)
    self.assertIs(self._module._original_safe_repr, None)

  def test_type_with_tag_true(self):
    class T:
      _COLAB_HAS_SAFE_REPR = True

      def __repr__(self):
        return 'repr_T'

    with mock.patch.object(self._module, '_original_safe_repr') as m:
      self.assertEqual(self._module._impl(T(), depth=0, visited=None), 'repr_T')

    m.assert_not_called()

  def test_ignore_type_with_tag_false(self):
    class T:
      _COLAB_HAS_SAFE_REPR = False

      def __repr__(self):
        raise NotImplementedError

    with mock.patch.object(self._module, '_original_safe_repr') as m:
      t = T()
      m.return_value = 'safe_repr_T'
      self.assertEqual(self._module._impl(t, depth=0), 'safe_repr_T')
    m.assert_called_once_with(t, depth=0)

  def test_ignore_type_without_tag(self):
    class T:

      def __repr__(self):
        raise NotImplementedError

    with mock.patch.object(self._module, '_original_safe_repr') as m:
      t = T()
      m.return_value = 'safe_repr_T'
      self.assertEqual(self._module._impl(t, depth=1), 'safe_repr_T')
    m.assert_called_once_with(t, depth=1)

  def test_ignore_object_with_tag(self):
    class T:

      def __repr__(self):
        raise NotImplementedError

    with mock.patch.object(self._module, '_original_safe_repr') as m:
      t = T()
      t._COLAB_HAS_SAFE_REPR = True  # pylint: disable=invalid-name
      m.return_value = 'safe_repr_T'
      self.assertEqual(
          self._module._impl(t, depth=1, visited=None), 'safe_repr_T'
      )
    m.assert_called_once_with(t, depth=1, visited=None)


if __name__ == '__main__':
  absltest.main()
