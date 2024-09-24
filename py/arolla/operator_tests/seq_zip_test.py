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

"""Tests for seq.zip operator."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

M = arolla.M

_VALUE_QTYPES = [
    arolla.BYTES,
    arolla.INT32,
    arolla.FLOAT32,
    arolla.DENSE_ARRAY_INT32,
]
_SEQUENCE_QTYPES = list(
    arolla.types.make_sequence_qtype(x) for x in _VALUE_QTYPES
)

# All qtypes that we consider existing in the qtype signatures test
_ALL_POSSIBLE_QTYPES = _SEQUENCE_QTYPES + list(
    pointwise_test_utils.DETECT_SIGNATURES_DEFAULT_QTYPES
)


def get_output_qtype(*args):
  return arolla.types.make_sequence_qtype(
      arolla.types.make_tuple_qtype(
          *[arolla.types.get_scalar_qtype(x) for x in args]
      )
  )


def gen_qtype_signatures():
  """Yields qtype signatures for seq.zip.

  Yields: (arg_qtype, result_qtype)
  """
  for arg_1_qtype in _ALL_POSSIBLE_QTYPES:
    if arolla.types.is_sequence_qtype(arg_1_qtype):
      yield (arg_1_qtype, get_output_qtype(arg_1_qtype))
      for arg_2_qtype in _ALL_POSSIBLE_QTYPES:
        if arolla.types.is_sequence_qtype(arg_2_qtype):
          yield (
              arg_1_qtype,
              arg_2_qtype,
              get_output_qtype(arg_1_qtype, arg_2_qtype),
          )
          for arg_3_qtype in _ALL_POSSIBLE_QTYPES:
            if arolla.types.is_sequence_qtype(arg_3_qtype):
              yield (
                  arg_1_qtype,
                  arg_2_qtype,
                  arg_3_qtype,
                  get_output_qtype(arg_1_qtype, arg_2_qtype, arg_3_qtype),
              )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class SeqZipTest(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.seq.zip,
        QTYPE_SIGNATURES,
        possible_qtypes=_SEQUENCE_QTYPES + _VALUE_QTYPES,
        max_arity=3,
    )

  def testSingleSequence(self):
    seq = arolla.types.Sequence(1, 2, 3)

    expected_qvalue = arolla.types.Sequence(
        arolla.eval(M.core.make_tuple(1)),
        arolla.eval(M.core.make_tuple(2)),
        arolla.eval(M.core.make_tuple(3)),
    )
    actual_qvalue = arolla.eval(M.seq.zip(seq))

    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_qvalue, expected_qvalue
    )

  def testValue(self):
    seq_a = arolla.types.Sequence(1, 2, 3, 4)
    seq_b = arolla.types.Sequence(5.0, 6.0, 7.0, 8.0)
    seq_c = arolla.types.Sequence(b'a', b'b', b'c', b'd')

    expected_qvalue = arolla.types.Sequence(
        arolla.eval(M.core.make_tuple(1, 5.0, b'a')),
        arolla.eval(M.core.make_tuple(2, 6.0, b'b')),
        arolla.eval(M.core.make_tuple(3, 7.0, b'c')),
        arolla.eval(M.core.make_tuple(4, 8.0, b'd')),
    )
    actual_qvalue = arolla.eval(M.seq.zip(seq_a, seq_b, seq_c))

    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_qvalue, expected_qvalue
    )

  def testErrorLengthMismatch(self):
    seq_a = arolla.types.Sequence(1, 2, 3, 4)
    seq_b = arolla.types.Sequence(5.0, 6.0)

    with self.assertRaisesRegex(
        ValueError, re.escape('all sequences should have equal sizes, 4 != 2')
    ):
      _ = arolla.eval(M.seq.zip(seq_a, seq_b))

  def testErrorEmptySignature(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('at least one argument is expected')
    ):
      _ = arolla.eval(M.seq.zip())


if __name__ == '__main__':
  absltest.main()
