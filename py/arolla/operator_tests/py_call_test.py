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

"""Tests for py.call operator."""

import re

from absl.testing import absltest
from arolla import arolla

M = arolla.M
L = arolla.L


class PyCallTest(absltest.TestCase):

  def test_eval(self):
    def fn(a, /, b, *c, d, **e):
      assert isinstance(c, tuple)
      assert isinstance(e, dict)
      return arolla.text(str(arolla.tuple(a, b, c, d, arolla.namedtuple(**e))))

    with self.subTest('runtime-eval'):
      value = arolla.eval(
          M.py.call(L.fn, arolla.TEXT, L.args, L.kwargs),
          fn=arolla.abc.PyObject(fn),
          args=arolla.tuple(0, 1, 2, arolla.int64(3)),
          kwargs=arolla.namedtuple(d=4, e=5, f=arolla.int64(6)),
      )
      self.assertEqual(
          value,
          '(0, 1, (2, int64{3}), 4, namedtuple<e=INT32,f=INT64>{(5,'
          ' int64{6})})',
      )

    with self.subTest('compiletime-eval'):
      value = arolla.eval(
          M.py.call(
              arolla.abc.PyObject(fn),
              arolla.TEXT,
              arolla.tuple(0, 1, 2, arolla.int64(3)),
              arolla.namedtuple(d=4, e=5, f=arolla.int64(6)),
          )
      )
      self.assertEqual(
          value,
          '(0, 1, (2, int64{3}), 4, namedtuple<e=INT32,f=INT64>{(5,'
          ' int64{6})})',
      )

  def test_nested_eval(self):
    def fn():
      return arolla.text('Test')

    def gn():
      return arolla.eval(
          M.py.call(L.fn, arolla.TEXT), fn=arolla.abc.PyObject(fn)
      )

    value = arolla.eval(
        M.py.call(L.gn, arolla.TEXT), gn=arolla.abc.PyObject(gn)
    )
    self.assertEqual(value, 'Test')

  def test_default_values(self):
    def fn(a=1, /, b=2.5, *c, d=b'd', **e):
      assert isinstance(a, int)
      assert isinstance(b, float)
      return arolla.text(str((a, b, c, d, e)))

    value = arolla.eval(M.py.call(arolla.abc.PyObject(fn), arolla.TEXT))
    self.assertEqual(value, "(1, 2.5, (), b'd', {})")

  def test_error_fn_return_non_qvalue(self):
    def fn():
      return object()

    expr = M.py.call(arolla.abc.PyObject(fn), arolla.TEXT, L.args, L.kwargs)
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected the result to be a qvalue, got object'
    ):
      _ = arolla.eval(expr, args=arolla.tuple(), kwargs=arolla.namedtuple())

  def test_error_fn_wrong_return_type(self):
    def fn():
      return arolla.int32(0)

    expr = M.py.call(arolla.abc.PyObject(fn), arolla.TEXT, L.args, L.kwargs)
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'expected the result to have qtype TEXT, got INT32'
    ):
      _ = arolla.eval(expr, args=arolla.tuple(), kwargs=arolla.namedtuple())

  def test_error_fn_raises(self):
    def fn():
      raise RuntimeError('Boom!')

    expr = M.py.call(arolla.abc.PyObject(fn), arolla.TEXT, L.args, L.kwargs)
    with self.assertRaisesWithLiteralMatch(RuntimeError, 'Boom!'):
      arolla.eval(expr, args=arolla.tuple(), kwargs=arolla.namedtuple())

  def test_error_fn_non_callable(self):
    expr = M.py.call(
        arolla.abc.PyObject(object()), arolla.TEXT, L.args, L.kwargs
    )
    with self.assertRaisesRegex(
        TypeError, re.escape("'object' object is not callable")
    ):
      _ = arolla.eval(expr, args=arolla.tuple(), kwargs=arolla.namedtuple(d=4))

  def test_error_fn_missign_arguments(self):
    def fn(a, /, b, *c, d, **e):
      return arolla.text(str(arolla.tuple(a, b, c, d, arolla.namedtuple(**e))))

    expr = M.py.call(arolla.abc.PyObject(fn), arolla.TEXT, L.args, L.kwargs)
    with self.assertRaisesRegex(
        TypeError,
        re.escape("missing 2 required positional arguments: 'a' and 'b'"),
    ):
      _ = arolla.eval(expr, args=arolla.tuple(), kwargs=arolla.namedtuple(d=4))
    with self.assertRaisesRegex(
        TypeError,
        re.escape("missing 1 required keyword-only argument: 'd'"),
    ):
      _ = arolla.eval(expr, args=arolla.tuple(1, 2), kwargs=arolla.namedtuple())

  def test_qtype_signature(self):
    self.assertEqual(
        repr(
            arolla.abc.infer_attr(
                M.py.call,
                (
                    arolla.abc.PY_OBJECT,
                    arolla.abc.Attr(qvalue=arolla.INT32),
                    arolla.make_tuple_qtype(),
                    arolla.make_namedtuple_qtype(),
                ),
            )
        ),
        'Attr(qtype=INT32)',
    )
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a PY_OBJECT, got fn: EXPR_OPERATOR')
    ):
      _ = arolla.abc.infer_attr(
          M.py.call,
          (
              arolla.abc.OPERATOR,
              arolla.abc.Attr(qvalue=arolla.INT32),
              arolla.make_tuple_qtype(),
              arolla.make_namedtuple_qtype(),
          ),
      )
    with self.assertRaisesRegex(
        ValueError, re.escape('expected return_type: QTYPE, got INT32')
    ):
      _ = arolla.abc.infer_attr(
          M.py.call,
          (
              arolla.abc.PY_OBJECT,
              arolla.INT32,
              arolla.make_tuple_qtype(),
              arolla.make_namedtuple_qtype(),
          ),
      )
    with self.assertRaisesRegex(
        ValueError, re.escape('`return_type` must be a literal')
    ):
      _ = arolla.abc.infer_attr(
          M.py.call,
          (
              arolla.abc.PY_OBJECT,
              arolla.QTYPE,
              arolla.make_tuple_qtype(),
              arolla.make_namedtuple_qtype(),
          ),
      )
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a tuple, got args: UNSPECIFIED')
    ):
      _ = arolla.abc.infer_attr(
          M.py.call,
          (
              arolla.abc.PY_OBJECT,
              arolla.QTYPE,
              arolla.UNSPECIFIED,
              arolla.make_namedtuple_qtype(),
          ),
      )
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a namedtuple, got kwargs: UNSPECIFIED')
    ):
      _ = arolla.abc.infer_attr(
          M.py.call,
          (
              arolla.abc.PY_OBJECT,
              arolla.QTYPE,
              arolla.make_tuple_qtype(),
              arolla.UNSPECIFIED,
          ),
      )


if __name__ == '__main__':
  absltest.main()
