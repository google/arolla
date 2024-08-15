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

"""Tests for M.array.present_indices."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M


def gen_test_data():
  """Yields test data for array.present_indices operator.

  Yields: tuple(
      (arg_qvalue, expected_qvalue),
      ...
  )
  """
  for array_fn in (arolla.array, arolla.dense_array):
    for scalar_qtype in arolla.types.SCALAR_QTYPES:
      # Empty.
      yield (
          array_fn([], value_qtype=scalar_qtype),
          array_fn([], value_qtype=arolla.INT64),
      )
      # One missing element.
      yield (
          array_fn([None], value_qtype=scalar_qtype),
          array_fn([], value_qtype=arolla.INT64),
      )
      # Sparse input.
      if scalar_qtype == arolla.BYTES:
        yield (
            array_fn([b'Foo', None, b'Bar', None], value_qtype=scalar_qtype),
            array_fn([0, 2], value_qtype=arolla.INT64),
        )
      elif scalar_qtype == arolla.TEXT:
        yield (
            array_fn(['Foo', None, 'Bar', None], value_qtype=scalar_qtype),
            array_fn([0, 2], value_qtype=arolla.INT64),
        )
      elif scalar_qtype == arolla.UNIT:
        yield (
            array_fn(
                [arolla.unit(), None, arolla.unit()], value_qtype=scalar_qtype
            ),
            array_fn([0, 2], value_qtype=arolla.INT64),
        )
      else:
        yield (
            array_fn([False, None, True], value_qtype=scalar_qtype),
            array_fn([0, 2], value_qtype=arolla.INT64),
        )


TEST_DATA = tuple(gen_test_data())

# QType signatures: tuple(
#     (arg_qtype, result_qtype),
#     ...
# )
QTYPE_SIGNATURES = frozenset(tuple(x.qtype for x in row) for row in TEST_DATA)


class ArrayPresentIndices(parameterized.TestCase):

  def testQTypeSignatures(self):
    arolla.testing.assert_qtype_signatures(
        M.array.present_indices, QTYPE_SIGNATURES
    )

  @parameterized.parameters(*TEST_DATA)
  def testValue(self, arg_qvalue, expected_qvalue):
    actual_qvalue = arolla.eval(M.array.present_indices(arg_qvalue))
    arolla.testing.assert_qvalue_allequal(actual_qvalue, expected_qvalue)


if __name__ == '__main__':
  absltest.main()
