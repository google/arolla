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

"""Tests for strings_agg_sum."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

L = arolla.L
M = arolla.M


def gen_qtype_signatures():
  for tpe in (arolla.types.BYTES, arolla.types.TEXT):
    optional_type = arolla.make_optional_qtype(tpe)
    for array_tpe, edge_tpe, scalar_edge_tpe in (
        (
            arolla.types.make_array_qtype(tpe),
            arolla.ARRAY_EDGE,
            arolla.ARRAY_TO_SCALAR_EDGE,
        ),
        (
            arolla.types.make_dense_array_qtype(tpe),
            arolla.DENSE_ARRAY_EDGE,
            arolla.DENSE_ARRAY_TO_SCALAR_EDGE,
        ),
    ):
      for sep_type in (tpe, arolla.UNSPECIFIED, optional_type):
        yield (array_tpe, edge_tpe, sep_type, array_tpe)
        yield (array_tpe, arolla.UNSPECIFIED, sep_type, array_tpe)
        yield (array_tpe, scalar_edge_tpe, sep_type, optional_type)

      yield (array_tpe, edge_tpe, array_tpe)
      yield (array_tpe, arolla.UNSPECIFIED, array_tpe)

      yield (array_tpe, scalar_edge_tpe, optional_type)
      yield (array_tpe, array_tpe)


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class StringsAggJoinTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):
  """Tests strings.agg_join operator on both BYTE and STRING arrays.

  Test is parameterized to test on all supported array types.
  """

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(QTYPE_SIGNATURES),
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.strings.agg_join)
        ),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_agg_join_text(self, array_factory):
    arolla_values = array_factory(
        ['the', 'quick', 'brown', 'fox', 'jumped', 'over', 'the', 'lazy', 'dog']
    )
    arolla_sizes = array_factory([4, 2, 3])
    edge = arolla.eval(M.edge.from_sizes(arolla_sizes))
    expected = array_factory(
        ['the quick brown fox', 'jumped over', 'the lazy dog']
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.agg_join(arolla_values, edge, sep=' ')), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_agg_join_bytes(self, array_factory):
    arolla_values = array_factory([
        b'the',
        b'quick',
        b'brown',
        b'fox',
        b'jumped',
        b'over',
        b'the',
        b'lazy',
        b'dog',
    ])
    arolla_sizes = array_factory([4, 2, 3])
    edge = arolla.eval(M.edge.from_sizes(arolla_sizes))
    expected = array_factory(
        [b'the quick brown fox', b'jumped over', b'the lazy dog']
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.agg_join(arolla_values, edge, sep=b' ')), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_agg_join_default_over(self, array_factory):
    arolla_values = array_factory(
        ['the', 'quick', 'brown', 'fox', 'jumped', 'over', 'the', 'lazy', 'dog']
    )
    expected = array_factory(['the quick brown fox jumped over the lazy dog'])
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.agg_join(arolla_values, sep=' ')), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_agg_join_default_sep(self, array_factory):
    arolla_values = array_factory(
        ['the', 'quick', 'brown', 'fox', 'jumped', 'over', 'the', 'lazy', 'dog']
    )
    expected = array_factory(['thequickbrownfoxjumpedoverthelazydog'])
    arolla.testing.assert_qvalue_allequal(
        self.eval(M.strings.agg_join(arolla_values)), expected
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_agg_join_empty_text_sep(self, array_factory):
    arolla_values = array_factory(['foo', 'bar', 'baz'])
    expr = M.strings.agg_join(arolla_values, sep=L.sep)
    arolla.testing.assert_qvalue_allequal(
        self.eval(expr, sep=arolla.optional_text(None)),
        array_factory(['foobarbaz']),
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(expr, sep=arolla.optional_text(',')),
        array_factory(['foo,bar,baz']),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def test_agg_join_empty_bytes_sep(self, array_factory):
    arolla_values = array_factory([b'foo', b'bar', b'baz'])
    expr = M.strings.agg_join(arolla_values, sep=L.sep)
    arolla.testing.assert_qvalue_allequal(
        self.eval(expr, sep=arolla.optional_bytes(None)),
        array_factory([b'foobarbaz']),
    )
    arolla.testing.assert_qvalue_allequal(
        self.eval(expr, sep=arolla.optional_bytes(b',')),
        array_factory([b'foo,bar,baz']),
    )


if __name__ == '__main__':
  absltest.main()
