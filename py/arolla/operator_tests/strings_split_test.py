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

"""Tests for M.strings.split."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base
from arolla.operator_tests import pointwise_test_utils
from arolla.operator_tests import utils

M = arolla.M


def gen_qtype_signatures():
  for s in pointwise_test_utils.lift_qtypes(
      arolla.TEXT,
      arolla.BYTES,
      mutators=pointwise_test_utils.ARRAY_QTYPE_MUTATORS,
  ):
    res = arolla.make_tuple_qtype(s, arolla.eval(M.qtype.get_edge_qtype(s)))
    yield s, res
    yield s, arolla.UNSPECIFIED, res
    yield s, arolla.types.get_scalar_qtype(s), res
    yield s, arolla.make_optional_qtype(arolla.types.get_scalar_qtype(s)), res


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class StringsSplitTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):
  def testQTypeSignatures(self):
    self.require_self_eval_is_called = False
    self.assertEqual(
        frozenset(
            pointwise_test_utils.detect_qtype_signatures(M.strings.split)
        ),
        frozenset(QTYPE_SIGNATURES),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testStringsSplit(self, array_factory):
    x = array_factory([None, 'hello world!',
                       'split with\n different\twhitespaces'])
    res = self.eval(M.strings.split(x))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        res,
        arolla.tuple(
            array_factory(
                ['hello', 'world!', 'split', 'with', 'different', 'whitespaces']
            ),
            self.eval(M.edge.from_sizes(array_factory([0, 2, 4]))),
        ),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testStringsSplitWithSeparator(self, array_factory):
    x = array_factory([
        'a..b..c',
        'split. right. do..wn. the. middle'])
    res = self.eval(M.strings.split(x, '..'))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        res,
        arolla.tuple(
            array_factory(
                ['a', 'b', 'c', 'split. right. do', 'wn. the. middle']
            ),
            self.eval(M.edge.from_sizes(array_factory([3, 2]))),
        ),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testStringsSplitEmpty(self, array_factory):
    x = array_factory([''])
    res = self.eval(M.strings.split(x))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        res,
        arolla.tuple(
            array_factory([], arolla.TEXT),
            self.eval(M.edge.from_sizes(array_factory([0]))),
        ),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testStringsSplitOnlySeparator(self, array_factory):
    x = array_factory([','])
    res = self.eval(M.strings.split(x, ','))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        res,
        arolla.tuple(
            array_factory(['', ''], arolla.TEXT),
            self.eval(M.edge.from_sizes(array_factory([2]))),
        ),
    )

  @parameterized.named_parameters(*utils.ARRAY_FACTORIES)
  def testStringsSplitIsInverseOfAggJoin(self, array_factory):
    original = array_factory([
        'Humpty Dumpty sat on bit',
        'Humpty Dumpty had a great split',
        'All the king\'s horses and all the king\'s men',
        'Did concatenate Humpty back together again!'])

    splits, edge = self.eval(M.strings.split(original, sep=' '))
    final = self.eval(M.strings.agg_join(splits, into=edge, sep=' '))
    arolla.testing.assert_qvalue_allequal(original, final)


if __name__ == '__main__':
  absltest.main()
