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

"""Tests for M.array.from_indices_and_values."""

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base

M = arolla.M


def gen_qtype_signatures():
  """Yields qtype signatures of array.from_indices_and_values operator."""
  for arg1_scalar_qtype in arolla.types.INTEGRAL_QTYPES:
    for arg2_scalar_qtype in arolla.types.SCALAR_QTYPES:
      for arg3_scalar_qtype in arolla.types.INTEGRAL_QTYPES:
        for array_qtype_fn in (
            arolla.make_dense_array_qtype,
            arolla.make_array_qtype,
        ):
          yield (
              array_qtype_fn(arg1_scalar_qtype),
              array_qtype_fn(arg2_scalar_qtype),
              arg3_scalar_qtype,
              array_qtype_fn(arg2_scalar_qtype),
          )


QTYPE_SIGNATURES = tuple(gen_qtype_signatures())


class ArrayFromIndicesAndValuesTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatues(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.array.from_indices_and_values, QTYPE_SIGNATURES
    )

  @parameterized.parameters(
      (
          arolla.array_int32([0, 1, 3, 5]),
          arolla.array_bytes([b'foo', b'bar', None, b'baz']),
          6,
          arolla.array_bytes([b'foo', b'bar', None, None, None, b'baz']),
      ),
      (
          arolla.array_int64([0, 1, 3, 5]),
          arolla.array_text(['foo', 'bar', None, 'baz']),
          6,
          arolla.array_text(['foo', 'bar', None, None, None, 'baz']),
      ),
      (
          arolla.array([0, 1, 3, 5]),
          arolla.array_boolean([True, False, None, True]),
          6,
          arolla.array_boolean([True, False, None, None, None, True]),
      ),
      (
          arolla.array([0, 1, 3, 5]),
          arolla.array_unit([True, True, None, True]),
          6,
          arolla.array_unit([True, True, None, None, None, True]),
      ),
  )
  def test_eval(self, indices, values, size, expected_qvalue):
    actual_qvalue = self.eval(
        M.array.from_indices_and_values(indices, values, size)
    )
    arolla.testing.assert_qvalue_allequal(actual_qvalue, expected_qvalue)

  def test_array_performance_no_huge_memory_allocations(self):
    # Make sure that all intermediate and output arrays are in the sparse form
    # and we don't actually allocate a buffer with 10**18 elements.
    actual_qvalue = self.eval(
        M.array.from_indices_and_values(
            indices=arolla.array_int64([0, 10**9, 10**18 - 1]),
            values=[0, 1, -1],
            size=arolla.int64(10**18),
        )
    )
    expected_qvalue = arolla.array(
        ids=[0, 10**9, 10**18 - 1],
        values=[0, 1, -1],
        size=arolla.int64(10**18),
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(
        actual_qvalue, expected_qvalue
    )

  @parameterized.parameters(arolla.dense_array, arolla.array)
  def test_error_negative_size(self, array_fn):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a non-negative integer, got size=-1')
    ):
      _ = self.eval(
          M.array.from_indices_and_values(
              indices=array_fn([1]), values=array_fn([1]), size=-1
          )
      )

  @parameterized.parameters(arolla.dense_array, arolla.array)
  def test_error_size_mismatch(self, array_fn):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected arrays of the same sizes, got '
            'indices.size=2, values.size=1'
        ),
    ):
      _ = self.eval(
          M.array.from_indices_and_values(
              indices=array_fn([1, 2]), values=array_fn([1]), size=10
          )
      )

  @parameterized.parameters(arolla.dense_array, arolla.array)
  def test_error_missing_index(self, array_fn):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError, re.escape('missing indices are not supported')
    ):
      _ = self.eval(
          M.array.from_indices_and_values(
              indices=array_fn([1, None]), values=array_fn([1, 1]), size=10
          )
      )

  @parameterized.parameters(arolla.dense_array, arolla.array)
  def test_error_negative_index(self, array_fn):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError, re.escape('expected non-negative indices, got index=-1')
    ):
      _ = self.eval(
          M.array.from_indices_and_values(
              indices=array_fn([-1]), values=array_fn([1]), size=10
          )
      )

  @parameterized.parameters(arolla.dense_array, arolla.array)
  def test_error_unordered_indices(self, array_fn):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a strictly increasing sequence of indices, '
            'got [..., 1, 0, ...]'
        ),
    ):
      _ = self.eval(
          M.array.from_indices_and_values(
              indices=array_fn([1, 0]), values=array_fn([1, 1]), size=10
          )
      )

  @parameterized.parameters(arolla.dense_array, arolla.array)
  def test_error_index_out_range(self, array_fn):
    self.require_self_eval_is_called = False
    with self.assertRaisesRegex(
        ValueError, re.escape('index is out of range, index=10 >= size=10')
    ):
      _ = self.eval(
          M.array.from_indices_and_values(
              indices=array_fn([10]), values=array_fn([1]), size=10
          )
      )


if __name__ == '__main__':
  absltest.main()
