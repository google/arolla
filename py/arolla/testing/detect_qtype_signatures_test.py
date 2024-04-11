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
      _ = tuple(
          detect_qtype_signatures.detect_qtype_signatures(op, max_arity=0)
      )

  def test_max_arity_with_variadic(self):
    possible_qtypes = (arolla_types.INT32, arolla_types.FLOAT32)
    op = arolla_types.LambdaOperator('*args', P.args)
    self.assertLen(
        tuple(
            detect_qtype_signatures.detect_qtype_signatures(
                op, possible_qtypes=possible_qtypes, max_arity=5
            )
        ),
        2**6 - 1,
    )
    with self.assertRaisesRegex(ValueError, re.escape('specify `max_arity`')):
      _ = tuple(
          detect_qtype_signatures.detect_qtype_signatures(
              op, possible_qtypes=possible_qtypes
          )
      )

  def test_max_arity_with_variadic_and_default(self):
    possible_qtypes = (arolla_types.INT32, arolla_types.FLOAT32)
    op = arolla_types.LambdaOperator(('i, f=, *args', 0.5), (P.i, P.f, P.args))
    self.assertLen(
        tuple(
            detect_qtype_signatures.detect_qtype_signatures(
                op, possible_qtypes=possible_qtypes, max_arity=1
            )
        ),
        2,
    )
    self.assertLen(
        tuple(
            detect_qtype_signatures.detect_qtype_signatures(
                op, possible_qtypes=possible_qtypes, max_arity=2
            )
        ),
        6,
    )
    self.assertLen(
        tuple(
            detect_qtype_signatures.detect_qtype_signatures(
                op, possible_qtypes=possible_qtypes, max_arity=3
            )
        ),
        14,
    )

  def test_error_no_qtype_signatures_found(self):
    op = arolla_types.BackendOperator(
        'operator_without_signatures',
        'unused_x',
        qtype_inference_expr=arolla_abc.NOTHING,
    )
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'found no supported qtype signatures'
    ):
      _ = tuple(detect_qtype_signatures.detect_qtype_signatures(op))

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
        _ = tuple(detect_qtype_signatures.detect_qtype_signatures(op))
    with mock.patch.object(
        arolla_abc,
        'get_operator_signature',
        return_value=arolla_abc.Signature(
            ((arolla_abc.SignatureParameter(('x', 'keyword-only', None)),), '')
        ),
    ):
      with self.assertRaises(AssertionError):
        _ = tuple(detect_qtype_signatures.detect_qtype_signatures(op))
    with mock.patch.object(
        arolla_abc,
        'get_operator_signature',
        return_value=arolla_abc.Signature((
            (arolla_abc.SignatureParameter(('x', 'variadic-keyword', None)),),
            '',
        )),
    ):
      with self.assertRaises(AssertionError):
        _ = tuple(detect_qtype_signatures.detect_qtype_signatures(op))


if __name__ == '__main__':
  absltest.main()
