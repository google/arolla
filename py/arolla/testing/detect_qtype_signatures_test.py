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

import itertools
import re
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import abc as arolla_abc
from arolla.expr import expr as arolla_expr
from arolla.testing import detect_qtype_signatures
from arolla.types import types as arolla_types

M = arolla_expr.OperatorsContainer()
P = arolla_expr.PlaceholderContainer()


class DetectQTypeSignaturesTest(parameterized.TestCase):

  def test_unary_operator(self):
    op = arolla_types.RestrictedLambdaOperator(
        'x',
        P.x & arolla_types.missing(),
        qtype_constraints=[
            ((P.x == arolla_types.FLOAT32) | (P.x == arolla_types.FLOAT64), '')
        ],
    )
    expected_qtype_signatures = {
        (arolla_types.FLOAT32, arolla_types.OPTIONAL_FLOAT32),
        (arolla_types.FLOAT64, arolla_types.OPTIONAL_FLOAT64),
    }
    self.assertCountEqual(
        detect_qtype_signatures.detect_qtype_signatures(op),
        expected_qtype_signatures,
    )

  def test_binary_operator(self):
    op = arolla_types.RestrictedLambdaOperator(
        'i, f',
        (P.i, P.f),
        qtype_constraints=[
            ((P.i == arolla_types.INT32) | (P.i == arolla_types.INT64), ''),
            ((P.f == arolla_types.FLOAT32) | (P.f == arolla_types.FLOAT64), ''),
        ],
    )
    expected_qtype_signatures = {
        (i, f, arolla_types.make_tuple_qtype(i, f))
        for i, f in itertools.product(
            (arolla_types.INT32, arolla_types.INT64),
            (arolla_types.FLOAT32, arolla_types.FLOAT64),
        )
    }
    self.assertCountEqual(
        detect_qtype_signatures.detect_qtype_signatures(op),
        expected_qtype_signatures,
    )

  def test_operator_with_default_arg(self):
    op = arolla_types.RestrictedLambdaOperator(
        ('i, f=', 0.5),
        (P.i, P.f),
        qtype_constraints=[
            ((P.i == arolla_types.INT32) | (P.i == arolla_types.INT64), ''),
            ((P.f == arolla_types.FLOAT32) | (P.f == arolla_types.FLOAT64), ''),
        ],
    )
    unary_signatures = {
        (i, arolla_types.make_tuple_qtype(i, arolla_types.FLOAT32))
        for i in (arolla_types.INT32, arolla_types.INT64)
    }
    binary_signatures = {
        (i, f, arolla_types.make_tuple_qtype(i, f))
        for i, f in itertools.product(
            (arolla_types.INT32, arolla_types.INT64),
            (arolla_types.FLOAT32, arolla_types.FLOAT64),
        )
    }
    self.assertCountEqual(
        detect_qtype_signatures.detect_qtype_signatures(op),
        unary_signatures | binary_signatures,
    )

  def test_max_arity_without_variadic(self):
    op = arolla_types.RestrictedLambdaOperator(
        ('i, f=', 0.5),
        (P.i, P.f),
        qtype_constraints=[
            ((P.i == arolla_types.INT32) | (P.i == arolla_types.INT64), ''),
            ((P.f == arolla_types.FLOAT32) | (P.f == arolla_types.FLOAT64), ''),
        ],
    )
    unary_signatures = {
        (i, arolla_types.make_tuple_qtype(i, arolla_types.FLOAT32))
        for i in (arolla_types.INT32, arolla_types.INT64)
    }
    binary_signatures = {
        (i, f, arolla_types.make_tuple_qtype(i, f))
        for i, f in itertools.product(
            (arolla_types.INT32, arolla_types.INT64),
            (arolla_types.FLOAT32, arolla_types.FLOAT64),
        )
    }
    self.assertCountEqual(
        detect_qtype_signatures.detect_qtype_signatures(op, max_arity=5),
        unary_signatures | binary_signatures,
    )
    self.assertCountEqual(
        detect_qtype_signatures.detect_qtype_signatures(op, max_arity=2),
        unary_signatures | binary_signatures,
    )
    self.assertCountEqual(
        detect_qtype_signatures.detect_qtype_signatures(op, max_arity=1),
        unary_signatures,
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'found no supported qtype signatures'
    ):
      detect_qtype_signatures.detect_qtype_signatures(op, max_arity=0)

  def test_max_arity_with_variadic(self):
    possible_qtypes = (arolla_types.INT32, arolla_types.FLOAT32)
    op = arolla_types.LambdaOperator('*args', P.args)
    self.assertLen(
        detect_qtype_signatures.detect_qtype_signatures(
            op, possible_qtypes=possible_qtypes, max_arity=5
        ),
        2**6 - 1,
    )
    with self.assertRaisesRegex(ValueError, re.escape('specify `max_arity`')):
      detect_qtype_signatures.detect_qtype_signatures(
          op, possible_qtypes=possible_qtypes
      )

  def test_max_arity_with_variadic_and_default(self):
    possible_qtypes = (arolla_types.INT32, arolla_types.FLOAT32)
    op = arolla_types.LambdaOperator(('i, f=, *args', 0.5), (P.i, P.f, P.args))
    self.assertLen(
        detect_qtype_signatures.detect_qtype_signatures(
            op, possible_qtypes=possible_qtypes, max_arity=1
        ),
        2,
    )
    self.assertLen(
        detect_qtype_signatures.detect_qtype_signatures(
            op, possible_qtypes=possible_qtypes, max_arity=2
        ),
        6,
    )
    self.assertLen(
        detect_qtype_signatures.detect_qtype_signatures(
            op, possible_qtypes=possible_qtypes, max_arity=3
        ),
        14,
    )

  def test_no_possible_qtypes(self):
    op_with_default = arolla_types.RestrictedLambdaOperator(('i=', 1), P.i)
    self.assertCountEqual(
        detect_qtype_signatures.detect_qtype_signatures(
            op_with_default, possible_qtypes=()
        ),
        [(arolla_types.INT32,)],
    )
    op_without_default = arolla_types.RestrictedLambdaOperator('i', P.i)
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'found no supported qtype signatures'
    ):
      detect_qtype_signatures.detect_qtype_signatures(
          op_without_default, possible_qtypes=()
      )

  def test_error_no_qtype_signatures_found(self):
    op_without_signatures = arolla_types.BackendOperator(
        'operator_without_signatures',
        'unused_x',
        qtype_inference_expr=arolla_abc.NOTHING,
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'found no supported qtype signatures'
    ):
      detect_qtype_signatures.detect_qtype_signatures(op_without_signatures)

  def test_error_unsupported_parameter_kinds(self):
    op = arolla_types.LambdaOperator('x', P.x)
    with mock.patch.object(
        arolla_abc,
        'get_operator_signature',
        return_value=arolla_abc.Signature((
            (arolla_abc.SignatureParameter(('x', 'positional-only', None)),),
            '',
        )),
    ):
      with self.assertRaises(AssertionError):
        detect_qtype_signatures.detect_qtype_signatures(op)
    with mock.patch.object(
        arolla_abc,
        'get_operator_signature',
        return_value=arolla_abc.Signature(
            ((arolla_abc.SignatureParameter(('x', 'keyword-only', None)),), '')
        ),
    ):
      with self.assertRaises(AssertionError):
        detect_qtype_signatures.detect_qtype_signatures(op)
    with mock.patch.object(
        arolla_abc,
        'get_operator_signature',
        return_value=arolla_abc.Signature((
            (arolla_abc.SignatureParameter(('x', 'variadic-keyword', None)),),
            '',
        )),
    ):
      with self.assertRaises(AssertionError):
        detect_qtype_signatures.detect_qtype_signatures(op)


class AssertQTypeSignaturesEqualTest(parameterized.TestCase):

  def test_assert_qtype_signatures_equal__success(self):
    detect_qtype_signatures.assert_qtype_signatures_equal([], [])
    detect_qtype_signatures.assert_qtype_signatures_equal(
        [(arolla_types.INT32, arolla_types.INT32)],
        [(arolla_types.INT32, arolla_types.INT32)],
    )
    detect_qtype_signatures.assert_qtype_signatures_equal(
        [
            (
                arolla_types.INT32,
                arolla_types.INT32,
            ),
            (
                arolla_types.FLOAT64,
                arolla_types.FLOAT64,
            ),
            (  # duplicate signature to test unique-ness
                arolla_types.FLOAT64,
                arolla_types.FLOAT64,
            ),
        ],
        [
            (
                arolla_types.FLOAT64,
                arolla_types.FLOAT64,
            ),
            (  # duplicate signature to test unique-ness
                arolla_types.FLOAT64,
                arolla_types.FLOAT64,
            ),
            (
                arolla_types.INT32,
                arolla_types.INT32,
            ),
        ],
    )

  def test_assert_qtype_signatures_equal__all_kinds_of_errors(self):
    detected = [
        (  # Matches
            arolla_types.INT32,
            arolla_types.INT32,
        ),
        (  # Matches
            arolla_types.INT64,
            arolla_types.INT64,
            arolla_types.INT64,
        ),
        (  # Changed output type
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
        ),
        (  # Changed output type
            arolla_types.FLOAT32,
            arolla_types.FLOAT64,
            arolla_types.FLOAT32,
        ),
        (  # Missing detected
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
        ),
        (  # Missing detected
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
        ),
    ]
    expected = [
        (  # Matches
            arolla_types.INT32,
            arolla_types.INT32,
        ),
        (  # Matches
            arolla_types.INT64,
            arolla_types.INT64,
            arolla_types.INT64,
        ),
        (  # Changed output type
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT64,
        ),
        (  # Changed output type
            arolla_types.FLOAT32,
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
        ),
        (  # Missing expected
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
        ),
        (  # Missing expected
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
        ),
    ]

    with self.assertRaisesRegex(
        AssertionError,
        re.escape("""Sets of qtype signatures are not equal:

  Unexpected result qtypes:
    (FLOAT32, FLOAT32) -> FLOAT32, expected FLOAT64
    (FLOAT32, FLOAT64) -> FLOAT32, expected FLOAT64

  The following qtype signatures expected, but not supported:
    (FLOAT64, FLOAT64, FLOAT64) -> FLOAT64
    (FLOAT64, FLOAT64, FLOAT64, FLOAT64) -> FLOAT64

  The following qtype signatures supported, but not expected:
    (FLOAT32, FLOAT32, FLOAT32) -> FLOAT32
    (FLOAT32, FLOAT32, FLOAT32, FLOAT32) -> FLOAT32"""),
    ):
      detect_qtype_signatures.assert_qtype_signatures_equal(detected, expected)

    with self.assertRaisesRegex(
        AssertionError,
        re.escape("""Sets of qtype signatures are not equal:

  Unexpected result qtypes:
    (FLOAT32, FLOAT32) -> FLOAT32, expected FLOAT64
    ... (1 more)

  The following qtype signatures expected, but not supported:
    (FLOAT64, FLOAT64, FLOAT64) -> FLOAT64
    ... (1 more)

  The following qtype signatures supported, but not expected:
    (FLOAT32, FLOAT32, FLOAT32) -> FLOAT32
    ... (1 more)"""),
    ):
      detect_qtype_signatures.assert_qtype_signatures_equal(
          detected, expected, max_errors_to_report=1
      )

    with self.assertRaisesRegex(AssertionError, 'foo'):
      detect_qtype_signatures.assert_qtype_signatures_equal(
          detected, expected, msg='foo'
      )

  def test_assert_qtype_signatures_equal__one_kind_of_errors(self):
    detected = [
        (  # Matches
            arolla_types.INT32,
            arolla_types.INT32,
        ),
        (  # Matches
            arolla_types.INT64,
            arolla_types.INT64,
            arolla_types.INT64,
        ),
        (  # Changed output type
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
        ),
        (  # Changed output type
            arolla_types.FLOAT32,
            arolla_types.FLOAT64,
            arolla_types.FLOAT32,
        ),
    ]
    expected = [
        (arolla_types.INT32, arolla_types.INT32),  # Matches
        (arolla_types.INT64, arolla_types.INT64, arolla_types.INT64),  # Matches
        (  # Changed output type
            arolla_types.FLOAT32,
            arolla_types.FLOAT32,
            arolla_types.FLOAT64,
        ),
        (  # Changed output type
            arolla_types.FLOAT32,
            arolla_types.FLOAT64,
            arolla_types.FLOAT64,
        ),
    ]

    with self.assertRaisesRegex(
        AssertionError,
        re.escape("""Sets of qtype signatures are not equal:

  Unexpected result qtypes:
    (FLOAT32, FLOAT32) -> FLOAT32, expected FLOAT64
    (FLOAT32, FLOAT64) -> FLOAT32, expected FLOAT64""") + '$',
    ):
      detect_qtype_signatures.assert_qtype_signatures_equal(detected, expected)

    with self.assertRaisesRegex(
        AssertionError,
        re.escape("""Sets of qtype signatures are not equal:

  Unexpected result qtypes:
    (FLOAT32, FLOAT32) -> FLOAT32, expected FLOAT64
    ... (1 more)""") + '$',
    ):
      detect_qtype_signatures.assert_qtype_signatures_equal(
          detected, expected, max_errors_to_report=1
      )

    with self.assertRaisesRegex(AssertionError, 'foo'):
      detect_qtype_signatures.assert_qtype_signatures_equal(
          detected, expected, msg='foo'
      )

  def test_assert_qtype_signatures_equal__wrong_input(self):
    with self.assertRaisesRegex(
        AssertionError,
        'duplicate input types found in actual signatures',
    ):
      detect_qtype_signatures.assert_qtype_signatures_equal(
          [
              (arolla_types.INT32, arolla_types.INT32),
              (arolla_types.INT32, arolla_types.FLOAT32),
          ],
          [],
      )

    with self.assertRaisesRegex(
        AssertionError,
        'duplicate input types found in expected signatures',
    ):
      detect_qtype_signatures.assert_qtype_signatures_equal(
          [],
          [
              (arolla_types.INT32, arolla_types.INT32),
              (arolla_types.INT32, arolla_types.FLOAT32),
          ],
      )


class AssertQTypeSignaturesTest(parameterized.TestCase):

  def test_assert_qtype_signatures(self):
    op = arolla_types.RestrictedLambdaOperator(
        'x',
        P.x & arolla_types.missing(),
        qtype_constraints=[
            ((P.x == arolla_types.FLOAT32) | (P.x == arolla_types.FLOAT64), '')
        ],
    )
    detect_qtype_signatures.assert_qtype_signatures(
        op,
        [
            (arolla_types.FLOAT32, arolla_types.OPTIONAL_FLOAT32),
            (arolla_types.FLOAT64, arolla_types.OPTIONAL_FLOAT64),
        ],
    )
    detect_qtype_signatures.assert_qtype_signatures(
        op,
        [
            (arolla_types.FLOAT32, arolla_types.OPTIONAL_FLOAT32),
        ],
        possible_qtypes=(arolla_types.FLOAT32,),
    )
    with self.assertRaisesRegex(  # pylint: disable=g-error-prone-assert-raises
        ValueError, 'found no supported qtype signatures'
    ):
      detect_qtype_signatures.assert_qtype_signatures(
          op,
          [],
          max_arity=0,
      )
    with self.assertRaisesRegex(
        AssertionError,
        'Sets of qtype signatures are not equal',
    ):
      detect_qtype_signatures.assert_qtype_signatures(
          op,
          [
              (arolla_types.FLOAT32, arolla_types.OPTIONAL_FLOAT64),
              (arolla_types.FLOAT64, arolla_types.OPTIONAL_FLOAT32),
          ],
      )


if __name__ == '__main__':
  absltest.main()
