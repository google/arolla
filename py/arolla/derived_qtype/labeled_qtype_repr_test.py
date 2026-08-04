# Copyright 2025 Google LLC
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

import inspect
import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.derived_qtype import derived_qtype

M = arolla.M | derived_qtype.M

BAR_OI32_QTYPE = arolla.eval(
    M.derived_qtype.get_labeled_qtype(arolla.OPTIONAL_INT32, 'bar')
)
BAR_OI32 = arolla.eval(
    M.derived_qtype.downcast(BAR_OI32_QTYPE, arolla.optional_int32(2))
)
BAR_F32_QTYPE = arolla.eval(
    M.derived_qtype.get_labeled_qtype(arolla.FLOAT32, 'bar')
)
BAR_F32 = arolla.eval(
    M.derived_qtype.downcast(BAR_F32_QTYPE, arolla.float32(2.0))
)


class LabeledQTypeReprTest(parameterized.TestCase):

  def test_register_custom_repr(self):
    foo_qtype = arolla.eval(
        M.derived_qtype.get_labeled_qtype(arolla.OPTIONAL_INT32, 'foo')
    )
    foo_oi32 = arolla.eval(
        M.derived_qtype.downcast(foo_qtype, arolla.optional_int32(2))
    )
    # Default repr.
    self.assertEqual(repr(foo_oi32), 'LABEL[foo]{optional_int32{2}}')

    def repr_fn(value):
      del value
      res = arolla.abc.ReprToken()
      res.text = 'my_custom_repr'
      return res

    # We can set the repr specifically for `'bar'`.
    derived_qtype.register_labeled_qtype_repr_fn('bar', repr_fn, override=True)
    self.assertEqual(repr(BAR_OI32), 'my_custom_repr')
    self.assertEqual(repr(BAR_F32), 'my_custom_repr')
    # This does not affect other labeled qtypes.
    self.assertEqual(repr(foo_oi32), 'LABEL[foo]{optional_int32{2}}')

  def test_repr_fn_none_fallback(self):
    def repr_fn(value):
      del value
      return None

    derived_qtype.register_labeled_qtype_repr_fn('bar', repr_fn, override=True)
    self.assertEqual(repr(BAR_OI32), 'LABEL[bar]{optional_int32{2}}')

  def test_repr_fn_warning_fallback(self):
    def repr_fn(value):
      del value
      raise ValueError('test')

    derived_qtype.register_labeled_qtype_repr_fn('bar', repr_fn, override=True)
    with self.assertWarnsRegex(
        RuntimeWarning,
        re.escape(
            'failed to evaluate the repr_fn on a value with'
            f' qtype={BAR_OI32_QTYPE.name} and'
            f' fingerprint={BAR_OI32.fingerprint}'
        ),
    ):
      repr_res = repr(BAR_OI32)
    self.assertEqual(repr_res, 'LABEL[bar]{optional_int32{2}}')

  def test_register_repr_fn_override(self):
    def repr_fn_1(value):
      del value
      res = arolla.abc.ReprToken()
      res.text = 'my_custom_repr_1'
      return res

    def repr_fn_2(value):
      del value
      res = arolla.abc.ReprToken()
      res.text = 'my_custom_repr_2'
      return res

    qtype = arolla.eval(
        M.derived_qtype.get_labeled_qtype(arolla.INT32, 'override_qtype')
    )
    qvalue = arolla.eval(M.derived_qtype.downcast(qtype, arolla.int32(2)))
    derived_qtype.register_labeled_qtype_repr_fn('override_qtype', repr_fn_1)
    self.assertEqual(repr(qvalue), 'my_custom_repr_1')
    with self.assertRaisesRegex(
        ValueError,
        "label 'override_qtype' already has a registered repr function",
    ):
      derived_qtype.register_labeled_qtype_repr_fn('override_qtype', repr_fn_2)

    derived_qtype.register_labeled_qtype_repr_fn(
        'override_qtype', repr_fn_2, override=True
    )
    self.assertEqual(repr(qvalue), 'my_custom_repr_2')

  def test_signature(self):
    def register_labeled_qtype_repr_fn(label, repr_fn, /, *, override=False):
      del label, repr_fn, override

    self.assertEqual(
        inspect.signature(derived_qtype.register_labeled_qtype_repr_fn),
        inspect.signature(register_labeled_qtype_repr_fn),
    )


if __name__ == '__main__':
  absltest.main()
