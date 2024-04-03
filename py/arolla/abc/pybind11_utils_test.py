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

"""Tests for pybind11_utils.h."""

from absl.testing import absltest
from arolla.abc import clib
from arolla.abc import testing_clib

UNSPECIFIED = clib.unspecified().qtype
OPERATOR = clib.unsafe_make_registered_operator('reg_op').qtype


class PyBind11UtilsTest(absltest.TestCase):

  def test_type_caster_load_fingerprint(self):
    testing_clib.pybind11_type_caster_load_fingerprint(
        clib.leaf('x').fingerprint
    )
    with self.assertRaises(TypeError):
      testing_clib.pybind11_type_caster_load_fingerprint(object())

  def test_type_caster_load_qvalue(self):
    testing_clib.pybind11_type_caster_load_qvalue(clib.unspecified())
    with self.assertRaises(TypeError):
      testing_clib.pybind11_type_caster_load_qvalue(object())

  def test_type_caster_load_qtype(self):
    testing_clib.pybind11_type_caster_load_qtype(UNSPECIFIED)
    with self.assertRaises(TypeError):
      testing_clib.pybind11_type_caster_load_qtype(clib.unspecified())
    with self.assertRaises(TypeError):
      testing_clib.pybind11_type_caster_load_qtype(object())

  def test_type_caster_load_operator(self):
    testing_clib.pybind11_type_caster_load_operator(
        clib.unsafe_make_registered_operator('reg_op')
    )
    with self.assertRaises(TypeError):
      testing_clib.pybind11_type_caster_load_operator(clib.unspecified())
    with self.assertRaises(TypeError):
      testing_clib.pybind11_type_caster_load_operator(object())

  def test_type_caster_load_expr(self):
    testing_clib.pybind11_type_caster_load_expr(clib.leaf('x'))
    with self.assertRaises(TypeError):
      testing_clib.pybind11_type_caster_load_expr(object())

  def test_type_caster_load_operator_signature(self):
    testing_clib.pybind11_type_caster_load_operator_signature(
        clib.get_operator_signature(
            clib.unsafe_make_registered_operator('annotation.qtype')
        )
    )
    with self.assertRaises(TypeError):
      testing_clib.pybind11_type_caster_load_operator_signature(object())

  def test_type_caster_cast_fingerprint(self):
    self.assertIsInstance(
        testing_clib.pybind11_type_caster_cast_fingerprint(), clib.Fingerprint
    )

  def test_type_caster_cast_qvalue(self):
    self.assertIsInstance(
        testing_clib.pybind11_type_caster_cast_qvalue(), clib.QValue
    )

  def test_type_caster_cast_qtype(self):
    self.assertIsInstance(
        testing_clib.pybind11_type_caster_cast_qtype(), clib.QType
    )

  def test_type_caster_cast_operator(self):
    self.assertIsInstance(
        testing_clib.pybind11_type_caster_cast_operator(), clib.QValue
    )
    self.assertEqual(
        testing_clib.pybind11_type_caster_cast_operator().qtype,
        clib.unsafe_make_registered_operator('reg_op').qtype,
    )

  def test_type_caster_cast_expr(self):
    self.assertIsInstance(
        testing_clib.pybind11_type_caster_cast_expr(), clib.Expr
    )

  def test_type_caster_cast_operator_signature(self):
    self.assertIsInstance(
        testing_clib.pybind11_type_caster_cast_operator_signature(),
        clib.Signature,
    )

  def test_type_caster_cast_qvalue_error(self):
    qtype = testing_clib.pybind11_type_caster_cast_qvalue().qtype

    class FailingQValue(clib.QValue):

      def _arolla_init_(self):
        raise RuntimeError('fail')

    clib.register_qvalue_specialization_by_qtype(qtype, FailingQValue)
    try:
      with self.assertRaisesWithLiteralMatch(RuntimeError, 'fail'):
        testing_clib.pybind11_type_caster_cast_qvalue()
    finally:
      clib.remove_qvalue_specialization_by_qtype(qtype)

  def test_type_caster_cast_operator_error(self):
    class FailingQValue(clib.QValue):

      def _arolla_init_(self):
        raise RuntimeError('fail')

    clib.register_qvalue_specialization_by_qtype(OPERATOR, FailingQValue)
    try:
      with self.assertRaisesWithLiteralMatch(RuntimeError, 'fail'):
        testing_clib.pybind11_type_caster_cast_operator()
    finally:
      clib.remove_qvalue_specialization_by_qtype(OPERATOR)

  def test_type_caster_cast_operator_signature_error(self):
    class FailingQValue(clib.QValue):

      def _arolla_init_(self):
        raise RuntimeError('fail')

    failing_sig = testing_clib.python_c_api_signature_from_signature((
        (('x', 'positional-or-keyword', clib.unspecified()),),
        '',
    ))
    clib.register_qvalue_specialization_by_qtype(UNSPECIFIED, FailingQValue)
    try:
      with self.assertRaisesWithLiteralMatch(RuntimeError, 'fail'):
        testing_clib.pybind11_type_caster_cast_load_operator_signature(
            failing_sig
        )
    finally:
      clib.remove_qvalue_specialization_by_qtype(UNSPECIFIED)


if __name__ == '__main__':
  absltest.main()
