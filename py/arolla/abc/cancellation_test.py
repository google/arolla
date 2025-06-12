# Copyright 2025 Google LLC
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
import signal
import threading
import time

from absl.testing import absltest
from arolla.abc import cancellation


class CancellationTest(absltest.TestCase):

  def test_cancellation_context(self):
    cancellation_context = cancellation.CancellationContext()
    self.assertFalse(cancellation_context.cancelled())
    cancellation_context.raise_if_cancelled()  # no exception
    self.assertRegex(
        repr(cancellation_context),
        r'<CancellationContext\(addr=0x[0-9a-f]+, cancelled=False\)>',
    )
    cancellation_context.cancel()
    self.assertTrue(cancellation_context.cancelled())
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED]'):
      cancellation_context.raise_if_cancelled()
    self.assertRegex(
        repr(cancellation_context),
        r'<CancellationContext\(addr=0x[0-9a-f]+, cancelled=True\)>',
    )

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

    cancellation.run_in_cancellation_context(cancellation_context, fn)

  def test_current_cancellation_context_without_context(self):
    self.assertIsNone(cancellation.current_cancellation_context())
    self.assertFalse(cancellation.cancelled())
    cancellation.raise_if_cancelled()  # no exception

  def test_run_in_cancellation_context_fn_args_kwargs_result(self):
    unique_token = object()

    def fn(a, *args, x, **kwargs):
      self.assertEqual(a, 'a')
      self.assertEqual(args, ('b', 'c'))
      self.assertEqual(x, 'x')
      self.assertEqual(kwargs, dict(y='y', z='z'))
      return unique_token

    self.assertIs(
        cancellation.run_in_cancellation_context(
            None, fn, 'a', 'b', 'c', x='x', y='y', z='z'
        ),
        unique_token,
    )

  def test_run_in_cancellation_context_fn_raises(self):
    def fn() -> str:
      raise RuntimeError('Boom!')

    with self.assertRaisesWithLiteralMatch(RuntimeError, 'Boom!'):
      cancellation.run_in_cancellation_context(None, fn)

  def test_run_in_cancellation_context_with_context_none(self):
    def fn():
      self.assertIsNone(cancellation.current_cancellation_context())

    cancellation_context = cancellation.CancellationContext()
    cancellation.run_in_cancellation_context(
        cancellation_context, cancellation.run_in_cancellation_context, None, fn
    )

  def test_run_in_cancellation_context_nesting(self):
    def fn():
      current_cancellation_context = cancellation.current_cancellation_context()
      self.assertIsNotNone(current_cancellation_context)
      current_cancellation_context.cancel()

    cancellation_context_1 = cancellation.CancellationContext()
    cancellation_context_2 = cancellation.CancellationContext()
    cancellation.run_in_cancellation_context(
        cancellation_context_1,
        cancellation.run_in_cancellation_context,
        cancellation_context_2,
        fn,
    )
    self.assertFalse(cancellation_context_1.cancelled())
    self.assertTrue(cancellation_context_2.cancelled())

  def test_run_in_cancellation_context_already_cancelled(self):
    cancellation_context = cancellation.CancellationContext()
    cancellation_context.cancel()
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED]'):
      cancellation.run_in_cancellation_context(
          cancellation_context, self.fail, msg='never called'
      )

  def test_run_in_cancellation_context_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.run_in_cancellation_context() missing 2 required positional'
        " arguments: 'cancellation_context', 'fn'",
    ):
      cancellation.run_in_cancellation_context()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.run_in_cancellation_context() missing 1 required positional'
        " argument: 'fn'",
    ):
      cancellation.run_in_cancellation_context(None)  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.run_in_cancellation_context() expected CancellationContext'
        ' or None, got cancellation_context: object',
    ):
      cancellation.run_in_cancellation_context(object(), lambda: None)  # pytype: disable=wrong-arg-types
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.run_in_cancellation_context() expected a callable, got fn:'
        ' object',
    ):
      cancellation.run_in_cancellation_context(None, object())  # pytype: disable=wrong-arg-types

  def test_run_in_default_cancellation_context_fn_args_kwargs_result(self):
    unique_token = object()

    def fn(a, *args, x, **kwargs):
      self.assertEqual(a, 'a')
      self.assertEqual(args, ('b', 'c'))
      self.assertEqual(x, 'x')
      self.assertEqual(kwargs, dict(y='y', z='z'))
      return unique_token

    self.assertIs(
        cancellation.run_in_default_cancellation_context(
            fn, 'a', 'b', 'c', x='x', y='y', z='z'
        ),
        unique_token,
    )

  def test_run_in_default_cancellation_context_fn_raises(self):
    def fn() -> str:
      raise RuntimeError('Boom!')

    with self.assertRaisesWithLiteralMatch(RuntimeError, 'Boom!'):
      cancellation.run_in_default_cancellation_context(fn)

  def test_run_in_default_cancellation_context_raise_signal(self):
    fn_done = False

    def fn():
      nonlocal fn_done
      self.assertFalse(cancellation.cancelled())
      with self.assertRaises(KeyboardInterrupt):
        signal.raise_signal(signal.SIGINT)
      time.sleep(0.1)  # allow worker thread time to process
      self.assertTrue(cancellation.cancelled())
      fn_done = True

    cancellation.run_in_default_cancellation_context(fn)
    self.assertTrue(fn_done)

  def test_run_in_default_cancellation_context_for_main_thread(self):
    fn_done = False

    def fn():
      nonlocal fn_done
      cancellation_context = cancellation.current_cancellation_context()
      self.assertIsNotNone(cancellation_context)
      self.assertFalse(cancellation_context.cancelled())
      cancellation.simulate_SIGINT()
      self.assertTrue(cancellation_context.cancelled())
      fn_done = True

    cancellation.run_in_default_cancellation_context(fn)
    self.assertTrue(fn_done)

  def test_run_in_default_cancellation_context_for_non_main_thread(self):
    fn_done = False

    def fn():
      nonlocal fn_done
      cancellation_context = cancellation.current_cancellation_context()
      self.assertIsNotNone(cancellation_context)
      self.assertFalse(cancellation_context.cancelled())
      cancellation.simulate_SIGINT()
      self.assertFalse(cancellation_context.cancelled())
      fn_done = True

    thread = threading.Thread(
        target=cancellation.run_in_default_cancellation_context, args=(fn,)
    )
    thread.start()
    thread.join()
    self.assertTrue(fn_done)

  def test_run_in_default_cancellation_context_with_nesting(self):
    fn_done = False
    cancellation_context = cancellation.CancellationContext()

    def fn():
      nonlocal fn_done
      nonlocal cancellation_context
      current_cancellation_context = cancellation.current_cancellation_context()
      self.assertIsNotNone(current_cancellation_context)
      self.assertFalse(current_cancellation_context.cancelled())
      cancellation_context.cancel()
      self.assertTrue(current_cancellation_context.cancelled())
      fn_done = True

    cancellation.run_in_cancellation_context(cancellation_context, fn)
    self.assertTrue(fn_done)

  def test_run_in_default_cancellation_context_already_cancelled(self):
    def fn():
      cancellation_context = cancellation.current_cancellation_context()
      self.assertIsNotNone(cancellation_context)
      cancellation_context.cancel('Boom!')
      cancellation.run_in_default_cancellation_context(
          self.fail, msg='never called'
      )

    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      cancellation.run_in_default_cancellation_context(fn)

  def test_run_in_default_cancellation_context_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.run_in_default_cancellation_context() missing 1 required'
        " positional arguments: 'fn'",
    ):
      cancellation.run_in_default_cancellation_context()  # pytype: disable=missing-parameter
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla.abc.run_in_default_cancellation_context() expected a callable,'
        ' got fn: object',
    ):
      cancellation.run_in_default_cancellation_context(object())  # pytype: disable=wrong-arg-types

  def test_add_default_cancellation_context_to_function(self):
    @cancellation.add_default_cancellation_context
    def fn(a, /, b=1, *c, d=2, **e):
      """Docstring."""
      cancellation_context = cancellation.current_cancellation_context()
      self.assertIsNotNone(cancellation_context)
      self.assertFalse(cancellation_context.cancelled())
      cancellation.simulate_SIGINT()
      self.assertTrue(cancellation_context.cancelled())
      return (a, b, c, d, e)

    self.assertIsNone(cancellation.current_cancellation_context())
    self.assertEqual(
        fn('a', 'b', 'c', x='x'), ('a', 'b', ('c',), 2, {'x': 'x'})
    )
    self.assertEqual(
        inspect.signature(fn),
        inspect.signature(lambda a, /, b=1, *c, d=2, **e: None),
    )
    self.assertEqual(fn.__doc__, 'Docstring.')
    self.assertEqual(fn.__name__, 'fn')

  def test_add_default_cancellation_context_to_method(self):
    test_self = self

    class A:

      @cancellation.add_default_cancellation_context
      def fn(self, /, b=1, *c, d=2, **e):
        """Docstring."""
        cancellation_context = cancellation.current_cancellation_context()
        test_self.assertIsNotNone(cancellation_context)
        test_self.assertFalse(cancellation_context.cancelled())
        cancellation.simulate_SIGINT()
        test_self.assertTrue(cancellation_context.cancelled())
        return (self, b, c, d, e)

    # Test bound method.
    a = A()
    self.assertIsNone(cancellation.current_cancellation_context())
    self.assertEqual(a.fn('b', 'c', x='x'), (a, 'b', ('c',), 2, {'x': 'x'}))
    self.assertEqual(
        inspect.signature(a.fn),
        inspect.signature(lambda b=1, *c, d=2, **e: None),
    )
    self.assertEqual(a.fn.__doc__, 'Docstring.')
    self.assertEqual(a.fn.__name__, 'fn')
    # Test unbound method.
    self.assertEqual(A.fn(a, 'b', 'c', x='x'), (a, 'b', ('c',), 2, {'x': 'x'}))
    self.assertEqual(
        inspect.signature(A.fn),
        inspect.signature(lambda self, /, b=1, *c, d=2, **e: None),
    )
    self.assertEqual(A.fn.__doc__, 'Docstring.')
    self.assertEqual(A.fn.__name__, 'fn')

  def test_unsafe_override_signal_handler_for_cancellation(self):
    try:
      with self.subTest('default_int_handler'):
        signal.signal(signal.SIGINT, signal.default_int_handler)
        cancellation.unsafe_override_signal_handler_for_cancellation()
        try:
          signal.raise_signal(signal.SIGINT)
        except KeyboardInterrupt:
          pass

      with self.subTest('my_int_handler'):
        my_int_handler_done = False

        def my_int_handler(signo, frame):
          del signo, frame
          nonlocal my_int_handler_done
          my_int_handler_done = True

        signal.signal(signal.SIGINT, my_int_handler)
        cancellation.unsafe_override_signal_handler_for_cancellation()
        signal.raise_signal(signal.SIGINT)  # no exception
        self.assertTrue(my_int_handler_done)

      with self.subTest('sig_ign'):
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        cancellation.unsafe_override_signal_handler_for_cancellation()
        signal.raise_signal(signal.SIGINT)  # no exception

      with self.subTest('cancellation'):
        signal.signal(signal.SIGINT, lambda *args: None)
        cancellation.unsafe_override_signal_handler_for_cancellation()
        _, _, cancelled = cancellation.run_in_default_cancellation_context(
            lambda: (
                signal.raise_signal(signal.SIGINT),
                time.sleep(0.1),  # allow worker thread time to process
                cancellation.cancelled(),
            )
        )
        self.assertTrue(cancelled)

    finally:
      signal.signal(signal.SIGINT, signal.default_int_handler)
      cancellation.unsafe_override_signal_handler_for_cancellation()

  def test_unsafe_override_signal_handler_for_cancellation_non_main_thread(
      self,
  ):
    fn_done = False

    def fn():
      nonlocal fn_done
      with self.assertRaisesWithLiteralMatch(
          ValueError, 'unsafe_set_signal_handler only works in main thread'
      ):
        cancellation.unsafe_override_signal_handler_for_cancellation()
      fn_done = True

    thread = threading.Thread(target=fn)
    thread.start()
    thread.join()
    self.assertTrue(fn_done)

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
        inspect.signature(cancellation.run_in_cancellation_context),
        inspect.signature(
            lambda cancellation_context, fn, /, *args, **kwargs: None
        ),
    )
    self.assertEqual(
        inspect.signature(cancellation.run_in_default_cancellation_context),
        inspect.signature(lambda fn, /, *args, **kwargs: None),
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
    self.assertEqual(
        inspect.signature(
            cancellation.unsafe_override_signal_handler_for_cancellation
        ),
        inspect.signature(lambda: None),
    )


if __name__ == '__main__':
  absltest.main()
