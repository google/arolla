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

"""Declaration of M.seq.* operators."""

from arolla import arolla
from arolla.operators.standard import core as M_core
from arolla.operators.standard import qtype as M_qtype

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints

# Bootstrap operators:
# go/keep-sorted start
map_ = arolla.abc.lookup_operator('seq.map')
reduce = arolla.abc.lookup_operator('seq.reduce')
# go/keep-sorted end


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'seq.at',
    qtype_constraints=[
        constraints.expect_sequence(P.seq),
        constraints.expect_scalar_integer(P.i),
    ],
    qtype_inference_expr=M_qtype.get_value_qtype(P.seq),
)
def at(seq, i):
  """Returns the i-th element of the sequence."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'seq.range',
    qtype_constraints=[
        constraints.expect_scalar_integer(P.arg_1),
        constraints.expect_integer(P.arg_2),
        constraints.expect_scalar_integer(P.step),
    ],
    qtype_inference_expr=M_qtype.make_sequence_qtype(arolla.INT64),
)
def range_(arg_1, arg_2=arolla.optional_int64(None), step=arolla.int64(1)):
  """range([start,] stop[, step]) returns evently spaced sequence of INT64."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'seq.size',
    qtype_constraints=[constraints.expect_sequence(P.seq)],
    qtype_inference_expr=arolla.INT64,
)
def size(seq):
  """Returns the sequence length."""
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    'seq.slice',
    qtype_constraints=[
        constraints.expect_sequence(P.seq),
        constraints.expect_scalar_integer(P.start),
        constraints.expect_scalar_integer(P.stop),
    ],
    qtype_inference_expr=P.seq,
)
def slice_(seq, start, stop):
  """Returns the slice of the given sequence.

  Example:
    >>> seq.slice(Sequence('a', 'b', 'c', 'd'), 1, 3)
    Sequence('b', 'c')

  Args:
    seq: sequence
    start: index of the first element
    stop: index after the last element
  """
  raise NotImplementedError('provided by backend')


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'seq.all',
    qtype_constraints=[(
        P.seq == M_qtype.make_sequence_qtype(arolla.OPTIONAL_UNIT),
        (
            'expected a sequence of optional units, '
            f'got {constraints.name_type_msg(P.seq)}'
        ),
    )],
)
def all_(seq):
  """Returns `present` iff all elements are present."""
  return reduce(M_core.presence_and, seq, arolla.optional_unit(True))


@arolla.optools.add_to_registry(unsafe_override=True)
@arolla.optools.as_lambda_operator(
    'seq.any',
    qtype_constraints=[(
        P.seq == M.qtype.make_sequence_qtype(arolla.OPTIONAL_UNIT),
        (
            'expected a sequence of optional units, '
            f'got {constraints.name_type_msg(P.seq)}'
        ),
    )],
)
def any_(seq):
  """Returns `present` iff any of the elements is present."""
  return M.seq.reduce(M.core.presence_or, seq, arolla.optional_unit(None))


@arolla.optools.add_to_registry()
@arolla.optools.as_lambda_operator(
    'seq.all_equal',
    qtype_constraints=[
        constraints.expect_sequence(P.seq),
        (
            M_qtype.is_scalar_qtype(M_qtype.get_value_qtype(P.seq))
            | M_qtype.is_optional_qtype(M_qtype.get_value_qtype(P.seq))
            | (M_qtype.get_value_qtype(P.seq) == arolla.QTYPE),
            (
                'expected scalar, optional or qtype, got'
                f' {constraints.name_type_msg(P.seq)}'
            ),
        ),
    ],
)
def all_equal(seq):
  """Returns whether all elements are present and equal."""
  # TODO: add support of the array types.
  return all_(
      map_(
          M_core.equal,
          slice_(seq, arolla.int64(0), arolla.int64(-1)),
          slice_(seq, arolla.int64(1), size(seq)),
      )
  )


@arolla.optools.add_to_registry()
@arolla.optools.as_backend_operator(
    name='seq.make',
    qtype_constraints=[(
        all_equal(M_qtype.get_field_qtypes(P.args)),
        (
            'arguments should be all of the same type, got'
            f' {constraints.variadic_name_type_msg(P.args)}'
        ),
    )],
    qtype_inference_expr=M_qtype.make_sequence_qtype(
        M_qtype.get_field_qtype(P.args, arolla.int64(0))
    ),
)
def make(*args):
  """Returns a sequence with the given elements."""
  raise NotImplementedError('provided by backend')
