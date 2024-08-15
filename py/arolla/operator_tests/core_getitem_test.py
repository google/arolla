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

"""Tests for M.core.getitem."""

import re
from absl.testing import absltest
from arolla import arolla

M = arolla.M
P = arolla.P


# Test overloading for the operator.
@arolla.optools.add_to_registry_as_overload(
    overload_condition_expr=(P.obj == arolla.types.PY_OBJECT)
)
@arolla.optools.as_py_function_operator(
    'core.getitem._py_object',
    qtype_inference_expr=arolla.types.PY_OBJECT,
)
def _getitem(obj, key):
  return arolla.types.PyObject(obj.py_value()[key.py_value()])


class CoreGetAttr(absltest.TestCase):
  """Tests operator evaluation."""

  def test_eval(self):
    obj = {'key': 'value'}
    expr = M.core.getitem(arolla.types.PyObject(obj), 'key')
    self.assertIs(arolla.eval(expr).py_value(), obj['key'])

  def test_error_no_overload(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('no matching overload [obj: FLOAT32, key: TEXT]')
    ):
      _ = M.core.getitem(1.5, 'nop')


if __name__ == '__main__':
  absltest.main()
