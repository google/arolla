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

"""Tests for the cancellation API."""

import inspect
import re

from absl.testing import absltest
from arolla.abc import cancellation


class CancellationTest(absltest.TestCase):

  def test_cancellation_context(self):
    cancellation_context = cancellation.CancellationContext()
    self.assertFalse(cancellation_context.cancelled())
    cancellation_context.raise_if_cancelled()  # no exception
    cancellation_context.cancel()
    self.assertTrue(cancellation_context.cancelled())
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED]'):
      cancellation_context.raise_if_cancelled()

  def test_cancellation_context_cancel_msg(self):
    cancellation_context = cancellation.CancellationContext()
    cancellation_context.cancel(msg='custom message')
    with self.assertRaisesWithLiteralMatch(
        ValueError, '[CANCELLED] custom message'
    ):
      cancellation_context.raise_if_cancelled()

  def test_cancellation_context_secondary_cancel(self):
    cancellation_context = cancellation.CancellationContext()
    cancellation_context.cancel('first message')
    cancellation_context.cancel('second message')
    with self.assertRaisesWithLiteralMatch(
        ValueError, '[CANCELLED] first message'
    ):
      cancellation_context.raise_if_cancelled()

  def test_cancellation_context_new_error(self):
    with self.assertRaisesRegex(
        TypeError,
        re.escape(
            'arolla.abc.CancellationContext.__new__() takes at most 0 arguments'
        ),
    ):
      cancellation.CancellationContext(object())  # pytype: disable=wrong-arg-count

  def test_cancellation_context_cancel_error(self):
    cancellation_context = cancellation.CancellationContext()
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "'message' is an invalid keyword argument for"
        ' arolla.abc.CancellationContext.cancel()',
    ):
      cancellation_context.cancel(message='')  # pytype: disable=wrong-keyword-args

  def test_current_cancellation_context(self):
    cancellation_context = cancellation.CancellationContext()

    def fn():
      current_cancellation_context = cancellation.current_cancellation_context()
      self.assertIsNotNone(current_cancellation_context)
      self.assertFalse(cancellation.cancelled())
      self.assertFalse(current_cancellation_context.cancelled())
      cancellation.raise_if_cancelled()  # no exception
      current_cancellation_context.raise_if_cancelled()  # no exception

      cancellation_context.cancel('unique-message')
      self.assertTrue(cancellation.cancelled())
      self.assertTrue(current_cancellation_context.cancelled())
      with self.assertRaisesWithLiteralMatch(
          ValueError, '[CANCELLED] unique-message'
      ):
        cancellation.raise_if_cancelled()
      with self.assertRaisesWithLiteralMatch(
          ValueError, '[CANCELLED] unique-message'
      ):
        current_cancellation_context.raise_if_cancelled()

    cancellation.call_with_cancellation(cancellation_context, fn)

  def test_current_cancellation_context_without_context(self):
    self.assertIsNone(cancellation.current_cancellation_context())
    self.assertFalse(cancellation.cancelled())
    cancellation.raise_if_cancelled()  # no exception

  def test_call_with_cancellation_fn_args_kwargs_result(self):
    unique_token = object()

    def fn(a, *args, x, **kwargs):
      self.assertEqual(a, 'a')
      self.assertEqual(args, ('b', 'c'))
      self.assertEqual(x, 'x')
      self.assertEqual(kwargs, dict(y='y', z='z'))
      return unique_token

    self.assertIs(
        cancellation.call_with_cancellation(
            None, fn, 'a', 'b', 'c', x='x', y='y', z='z'
        ),
        unique_token,
    )

  def test_call_with_cancellation_fn_raises(self):
    def fn() -> str:
      raise RuntimeError('Boom!')

    with self.assertRaisesWithLiteralMatch(RuntimeError, 'Boom!'):
      cancellation.call_with_cancellation(None, fn)

  def test_call_with_cancellation_with_context_none(self):
    def fn():
      self.assertIsNone(cancellation.current_cancellation_context())

    cancellation_context = cancellation.CancellationContext()
    cancellation.call_with_cancellation(
        cancellation_context, cancellation.call_with_cancellation, None, fn
    )

  def test_call_with_cancellation_nesting(self):
    def fn():
      current_cancellation_context = cancellation.current_cancellation_context()
      self.assertIsNotNone(current_cancellation_context)
      current_cancellation_context.cancel()

    cancellation_context_1 = cancellation.CancellationContext()
    cancellation_context_2 = cancellation.CancellationContext()
    cancellation.call_with_cancellation(
        cancellation_context_1,
        cancellation.call_with_cancellation,
        cancellation_context_2,
        fn,
    )
    self.assertFalse(cancellation_context_1.cancelled())
    self.assertTrue(cancellation_context_2.cancelled())

  def test_call_with_cancellation_already_cancelled(self):
    cancellation_context = cancellation.CancellationContext()
    cancellation_context.cancel()
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED]'):
      cancellation.call_with_cancellation(
          cancellation_context, self.fail, msg='never called'
      )

  def test_call_with_cancellation_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.call_with_cancellation() missing 2 required positional'
        " arguments: 'cancellation_context', 'fn'",
    ):
      cancellation.call_with_cancellation()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.call_with_cancellation() missing 1 required positional'
        " argument: 'fn'",
    ):
      cancellation.call_with_cancellation(None)  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.call_with_cancellation() expected CancellationContext or'
        ' None, got cancellation_context: object',
    ):
      cancellation.call_with_cancellation(object(), lambda: None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.call_with_cancellation() expected a callable, got fn:'
        ' object',
    ):
      cancellation.call_with_cancellation(None, object())  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.call_with_cancellation() expected a callable, got fn:'
        ' object',
    ):
      cancellation.call_with_cancellation(None, object())  # pytype: disable=wrong-arg-types

  def test_signatures(self):
    cancellation_context = cancellation.CancellationContext()
    self.assertEqual(
        inspect.signature(cancellation_context.cancel),
        inspect.signature(lambda msg='': None),
    )
    self.assertEqual(
        inspect.signature(cancellation_context.cancelled),
        inspect.signature(lambda: None),
    )
    self.assertEqual(
        inspect.signature(cancellation_context.raise_if_cancelled),
        inspect.signature(lambda: None),
    )
    self.assertEqual(
        inspect.signature(cancellation.call_with_cancellation),
        inspect.signature(
            lambda cancellation_context, fn, /, *args, **kwargs: None
        ),
    )
    self.assertEqual(
        inspect.signature(cancellation.current_cancellation_context),
        inspect.signature(lambda: None),
    )
    self.assertEqual(
        inspect.signature(cancellation.cancelled),
        inspect.signature(lambda: None),
    )
    self.assertEqual(
        inspect.signature(cancellation.raise_if_cancelled),
        inspect.signature(lambda: None),
    )


if __name__ == '__main__':
  absltest.main()
