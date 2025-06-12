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

"""Tests for arolla.abc.expr_substitution."""

from absl.testing import absltest
from arolla.abc import expr as abc_expr
from arolla.abc import expr_substitution as abc_expr_substitution
from arolla.abc import testing_clib


l_a = abc_expr.leaf('a')
l_b = abc_expr.leaf('b')
l_c = abc_expr.leaf('c')

p_a = abc_expr.placeholder('a')
p_b = abc_expr.leaf('b')


class ExprSubstitutionTest(absltest.TestCase):

  def test_sub_by_name(self):
    input_expr = abc_expr.unsafe_parse_sexpr(
        ('core.equal', l_a, testing_clib.with_name_annotation(l_b, 'a'))
    )
    expected_expr = abc_expr.unsafe_parse_sexpr(('core.equal', l_a, l_c))
    self.assertEqual(
        abc_expr_substitution.sub_by_name(input_expr, a=l_c).fingerprint,
        expected_expr.fingerprint,
    )

  def test_sub_leaves(self):
    input_expr = abc_expr.unsafe_parse_sexpr(
        ('core.equal', l_a, testing_clib.with_name_annotation(l_b, 'a'))
    )
    expected_expr = abc_expr.unsafe_parse_sexpr(
        ('core.equal', l_c, testing_clib.with_name_annotation(l_b, 'a'))
    )
    self.assertEqual(
        abc_expr_substitution.sub_leaves(input_expr, a=l_c).fingerprint,
        expected_expr.fingerprint,
    )

  def test_sub_placeholders(self):
    expr = abc_expr.unsafe_parse_sexpr(
        ('core.equal', p_a, testing_clib.with_name_annotation(p_b, 'a'))
    )
    expected_expr = abc_expr.unsafe_parse_sexpr(
        ('core.equal', l_c, testing_clib.with_name_annotation(p_b, 'a'))
    )
    self.assertEqual(
        abc_expr_substitution.sub_placeholders(expr, a=l_c).fingerprint,
        expected_expr.fingerprint,
    )

  def test_sub_by_fingerprint_simple(self):
    expr = abc_expr.unsafe_parse_sexpr((
        'core.equal',
        ('core.equal', p_a, testing_clib.with_name_annotation(p_b, 'a')),
        l_a,
    ))
    expected_expr = abc_expr.unsafe_parse_sexpr((
        'core.equal',
        ('core.equal', p_a, testing_clib.with_name_annotation(p_a, 'a')),
        l_b,
    ))
    self.assertEqual(
        abc_expr_substitution.sub_by_fingerprint(
            expr, {p_b.fingerprint: p_a, l_a.fingerprint: l_b}
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_sub_by_fingerprint_uses_original_expr(self):
    expr = abc_expr.unsafe_parse_sexpr(('core.equal', l_a, l_a))
    expected_expr = l_c
    # l.a -> l.b is a valid substitution, but (l.a == l.a) -> l.c trumps it.
    self.assertEqual(
        abc_expr_substitution.sub_by_fingerprint(
            expr,
            {
                l_a.fingerprint: l_b,
                abc_expr.unsafe_parse_sexpr(
                    ('core.equal', l_a, l_a)
                ).fingerprint: l_c,
            },
        ).fingerprint,
        expected_expr.fingerprint,
    )

  def test_sub_by_fingerprint_no_deep_substitution(self):
    expr = l_a
    expected_expr = l_b
    self.assertEqual(
        abc_expr_substitution.sub_by_fingerprint(
            expr, {l_a.fingerprint: l_b, l_b.fingerprint: l_c}
        ).fingerprint,
        expected_expr.fingerprint,
    )


if __name__ == '__main__':
  absltest.main()
