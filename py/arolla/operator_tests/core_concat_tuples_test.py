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

"""Tests for M.core.concat_tuples."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M
P = arolla.P

ELEMENT_QVALUES = (arolla.float32(0.0), arolla.int32(1), arolla.bytes_(b'2'))


def gen_test_data():
  for i in range(len(ELEMENT_QVALUES) + 1):
    for j in range(len(ELEMENT_QVALUES) + 1):
      yield (
          arolla.tuple(*ELEMENT_QVALUES[:i]),
          arolla.tuple(*ELEMENT_QVALUES[:j]),
          arolla.tuple(*(ELEMENT_QVALUES[:i] + ELEMENT_QVALUES[:j])),
      )


class CoreConcatTuplesTest(parameterized.TestCase):

  @parameterized.parameters(
      tuple(),
      (tuple(),),
      (tuple(), tuple()),
      (tuple(), tuple(), tuple()),
      ((arolla.float32(0.0),), tuple()),
      (tuple(), (arolla.float32(0.0),)),
      ((arolla.float32(0.0), arolla.int32(1)), (arolla.bytes_(b'2'),)),
      ((arolla.float32(0.0), arolla.int32(1)), tuple(), (arolla.bytes_(b'2'),)),
      ((arolla.float32(0.0), arolla.int32(1)), (arolla.bytes_(b'2'),), tuple()),
      (
          (arolla.float32(0.0), arolla.int32(1)),
          (arolla.bytes_(b'2'),),
          (arolla.float32(0.0),),
      ),
  )
  def testBehavior(self, *args):
    expected = arolla.tuple(*sum(args, tuple()))
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        arolla.eval(M.core.concat_tuples(*[arolla.tuple(*x) for x in args])),
        expected,
    )

  def testErrors(self):
    with self.assertRaisesRegex(ValueError, r'expected a tuple, got FLOAT32'):
      M.core.concat_tuples(
          arolla.float32(0.0),
          arolla.tuple(arolla.int32(1), arolla.bytes_(b'2')),
      )
    with self.assertRaisesRegex(ValueError, r'expected a tuple, got BYTES'):
      M.core.concat_tuples(
          arolla.tuple(arolla.float32(0.0), arolla.int32(1)),
          arolla.bytes_(b'2'),
      )


if __name__ == '__main__':
  absltest.main()
