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

import gc
import threading
import time
import weakref

from absl.testing import absltest
from arolla.experimental._tasks import clib
from arolla.experimental._tasks import interrupt


def _raise_pending_async_exc() -> None:
  """Executes python code so any pending async exception is raised."""
  for _ in range(10):
    pass


class _CustomInterruptError(BaseException):
  pass


class InterruptTest(absltest.TestCase):

  def test_init_rejects_arguments(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla_tasks.InterruptionScope() takes at most 0 arguments (1 given)',
    ):
      interrupt.InterruptionScope(1)  # pyrefly: ignore[bad-argument-count]

  def test_run_returns_value_and_forwards_args(self):
    interruption_scope = interrupt.InterruptionScope()
    self.assertEqual(
        interruption_scope.run(lambda a, b, *, c: a + b * c, 2, 3, c=4), 14
    )

  def test_run_propagates_exception(self):
    def fail():
      raise ValueError('BOOM!')

    interruption_scope = interrupt.InterruptionScope()
    with self.assertRaisesWithLiteralMatch(ValueError, 'BOOM!'):
      interruption_scope.run(fail)

  def test_run_cannot_be_called_twice(self):
    interruption_scope = interrupt.InterruptionScope()

    def work():
      with self.assertRaisesWithLiteralMatch(
          RuntimeError, 'InterruptionScope.run() can only be called once'
      ):
        interruption_scope.run(lambda: 42)
      return 42

    self.assertEqual(interruption_scope.run(work), 42)
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'InterruptionScope.run() can only be called once'
    ):
      interruption_scope.run(lambda: 42)

  def test_run_requires_callable(self):
    interruption_scope = interrupt.InterruptionScope()
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "InterruptionScope.run() missing 1 required positional argument: 'fn'",
    ):
      interruption_scope.run()  # pyrefly: ignore[bad-argument-count]
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'InterruptionScope.run() expected a callable, got fn: int',
    ):
      interruption_scope.run(123)  # pyrefly: ignore[bad-argument-type]

  def test_interrupt_injects_async_exceptions(self):
    interruption_scope = interrupt.InterruptionScope()
    started = threading.Event()
    first_caught = threading.Event()
    second_caught = threading.Event()

    def work():
      try:
        started.set()
        while True:
          time.sleep(0.005)
      except KeyboardInterrupt:
        try:
          first_caught.set()
          while True:
            time.sleep(0.005)
        except _CustomInterruptError:
          second_caught.set()

    thread = threading.Thread(target=interruption_scope.run, args=(work,))
    thread.start()
    self.assertTrue(started.wait(timeout=1.0))

    interruption_scope.interrupt()  # Defaults to KeyboardInterrupt.
    self.assertTrue(first_caught.wait(timeout=1.0))

    interruption_scope.interrupt(_CustomInterruptError)
    self.assertTrue(second_caught.wait(timeout=1.0))
    thread.join(timeout=1.0)

  def test_interrupt_outside_run_is_noop(self):
    interruption_scope = interrupt.InterruptionScope()
    interruption_scope.interrupt(_CustomInterruptError)
    self.assertEqual(interruption_scope.run(lambda: 42), 42)
    interruption_scope.interrupt()
    _raise_pending_async_exc()

  def test_unconsumed_async_exception_is_cleared_on_exit(self):
    interruption_scope = interrupt.InterruptionScope()
    # `interruption_scope.interrupt` injects `KeyboardInterrupt` and returns
    # before the exception is raised by the Python interpreter.
    interruption_scope.run(interruption_scope.interrupt)
    _raise_pending_async_exc()

  def test_interrupt_with_invalid_exception_raises_type_error(self):
    interruption_scope = interrupt.InterruptionScope()
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        '`exception_type` must be a BaseException subclass, got'
        ' KeyboardInterrupt()',
    ):
      interruption_scope.interrupt(
          KeyboardInterrupt()  # pyrefly: ignore[bad-argument-type]
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "`exception_type` must be a BaseException subclass, got <class 'int'>",
    ):
      interruption_scope.interrupt(int)  # pyrefly: ignore[bad-argument-type]

  def test_subscribe_to_interrupt(self):
    interruption_scope = interrupt.InterruptionScope()
    calls = []

    def work():
      with interrupt.subscribe_to_interrupt(lambda: calls.append('a')) as val:
        self.assertIsNone(val)
        interruption_scope.interrupt()
        _raise_pending_async_exc()
      # Active subscriptions also consume custom `exception_type` rather than
      # deferring it until exit, while still validating `exception_type`.
      with interrupt.subscribe_to_interrupt(lambda: calls.append('b')):
        interruption_scope.interrupt(_CustomInterruptError)
        _raise_pending_async_exc()
        with self.assertRaises(TypeError):
          interruption_scope.interrupt(int)  # pyrefly: ignore[bad-argument-type]
      _raise_pending_async_exc()
      with self.assertRaises(_CustomInterruptError):
        interruption_scope.interrupt(_CustomInterruptError)
        _raise_pending_async_exc()

    interruption_scope.run(work)
    self.assertEqual(calls, ['a', 'b'])

  def test_subscribe_to_interrupt_unsubscribes_on_exception(self):
    interruption_scope = interrupt.InterruptionScope()
    calls = []

    def work():
      with self.assertRaisesWithLiteralMatch(ValueError, 'Boom!'):
        with interrupt.subscribe_to_interrupt(lambda: calls.append('cb')):
          raise ValueError('Boom!')
      with self.assertRaises(KeyboardInterrupt):
        interruption_scope.interrupt()
        _raise_pending_async_exc()

    interruption_scope.run(work)
    self.assertEqual(calls, [])

  def test_subscribe_to_interrupt_cross_thread(self):
    interruption_scope = interrupt.InterruptionScope()
    ready = threading.Event()
    interrupted = threading.Event()
    callback_threads = []

    def on_interrupt():
      callback_threads.append(threading.get_ident())
      interrupted.set()

    def work():
      with interrupt.subscribe_to_interrupt(on_interrupt):
        ready.set()
        interrupted.wait(timeout=1.0)
      _raise_pending_async_exc()

    thread = threading.Thread(target=interruption_scope.run, args=(work,))
    thread.start()
    self.assertTrue(ready.wait(timeout=1.0))
    interruption_scope.interrupt()
    thread.join(timeout=1.0)
    self.assertTrue(interrupted.is_set())
    self.assertEqual(callback_threads, [threading.get_ident()])

  def test_nested_subscribe_to_interrupt(self):
    interruption_scope = interrupt.InterruptionScope()
    calls = []

    def work():
      with interrupt.subscribe_to_interrupt(lambda: calls.append('outer')):
        with interrupt.subscribe_to_interrupt(lambda: calls.append('middle')):
          with interrupt.subscribe_to_interrupt(lambda: calls.append('inner')):
            interruption_scope.interrupt()
          interruption_scope.interrupt()
        interruption_scope.interrupt()

    interruption_scope.run(work)
    self.assertEqual(
        calls, ['inner', 'middle', 'outer', 'middle', 'outer', 'outer']
    )

  def test_subscribe_to_interrupt_activates_on_enter_not_creation(self):
    calls = []
    with interrupt.subscribe_to_interrupt(lambda: calls.append('noop')) as val:
      self.assertIsNone(val)

    interruption_scope = interrupt.InterruptionScope()
    cm_from_outside = interrupt.subscribe_to_interrupt(
        lambda: calls.append('from_outside')
    )

    def work():
      cm_unentered = interrupt.subscribe_to_interrupt(
          lambda: calls.append('unentered')
      )
      with self.assertRaises(KeyboardInterrupt):
        interruption_scope.interrupt()
        _raise_pending_async_exc()
      with cm_from_outside:
        interruption_scope.interrupt()
      del cm_unentered

    interruption_scope.run(work)
    self.assertEqual(calls, ['from_outside'])

  def test_subscribe_to_interrupt_non_callable_raises_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'arolla_tasks.subscribe_to_interrupt() expected a callable, got'
        ' callback: int',
    ):
      interrupt.subscribe_to_interrupt(123)  # pyrefly: ignore[bad-argument-type]

  def test_nested_interruption_scopes(self):
    outer = interrupt.InterruptionScope()
    inner = interrupt.InterruptionScope()
    events = []

    def run_inner():
      with interrupt.subscribe_to_interrupt(lambda: events.append('inner')):
        inner.interrupt()
        outer.interrupt()
        raise ValueError('inner boom')

    def run_outer():
      with interrupt.subscribe_to_interrupt(lambda: events.append('outer1')):
        with self.assertRaisesWithLiteralMatch(ValueError, 'inner boom'):
          inner.run(run_inner)
        # `active_interruption_scope` is restored to `outer`.
        with interrupt.subscribe_to_interrupt(lambda: events.append('outer2')):
          outer.interrupt()

    outer.run(run_outer)
    self.assertEqual(events, ['inner', 'outer1', 'outer2', 'outer1'])

  def test_subscribe_to_interrupt_callback_raises(self):
    interruption_scope = interrupt.InterruptionScope()
    calls = []

    def bad_cb():
      raise ValueError('Boom!')

    def work():
      with (
          interrupt.subscribe_to_interrupt(lambda: calls.append('outer')),
          interrupt.subscribe_to_interrupt(bad_cb),
          interrupt.subscribe_to_interrupt(lambda: calls.append('inner')),
      ):
        with self.assertRaisesWithLiteralMatch(ValueError, 'Boom!'):
          interruption_scope.interrupt()
        with self.assertRaisesWithLiteralMatch(ValueError, 'Boom!'):
          interruption_scope.interrupt()

    interruption_scope.run(work)
    self.assertEqual(calls, ['inner', 'inner'])

  def test_subscribe_to_interrupt_raises_pending_async_exception_on_enter(self):
    interruption_scope = interrupt.InterruptionScope()
    calls = []

    class _InjectOnDel:
      # C callable so `__del__` does not execute Python bytecode that would
      # consume the injected async exception before `with cm:`.
      __del__ = staticmethod(interruption_scope.interrupt)

    def work():
      cm = interrupt.subscribe_to_interrupt(lambda: calls.append('cb'))
      trigger = _InjectOnDel()
      with self.assertRaises(KeyboardInterrupt):
        del trigger
        with cm:
          calls.append('body')
      with self.assertRaises(_CustomInterruptError):
        interruption_scope.interrupt(_CustomInterruptError)
        _raise_pending_async_exc()

    interruption_scope.run(work)
    self.assertEqual(calls, [])

  def test_subscribe_to_interrupt_cleaned_up_on_scope_exit(self):
    interruption_scope = interrupt.InterruptionScope()
    calls = []

    def make_gen(cm):
      with cm:
        yield

    def work():
      def leaked_cb():
        calls.append('leaked')

      gen = make_gen(interrupt.subscribe_to_interrupt(leaked_cb))
      next(gen)
      return gen, weakref.ref(leaked_cb)

    gen, cb_ref = interruption_scope.run(work)
    interruption_scope.interrupt()
    gen.close()
    del gen
    gc.collect()
    self.assertIsNone(cb_ref())
    self.assertEqual(calls, [])

  def test_subscribe_to_interrupt_cycle_is_gc_collectable(self):
    class _Holder:

      def __init__(self, scope=None):
        self.scope = scope
        self.cm = interrupt.subscribe_to_interrupt(self.on_interrupt)

      def on_interrupt(self):
        pass

    holder = _Holder()
    holder_ref = weakref.ref(holder)
    del holder
    gc.collect()
    self.assertIsNone(holder_ref())

    scope = interrupt.InterruptionScope()
    entered = _Holder(scope)
    entered_ref = weakref.ref(entered)

    def enter_and_exit():
      with entered.cm:
        pass

    scope.run(enter_and_exit)
    del entered, scope
    gc.collect()
    self.assertIsNone(entered_ref())

  def test_clib_interruption_subscription_overwrite(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        'InterruptionSubscription() takes at most 0 arguments (1 given)',
    ):
      clib.InterruptionSubscription(1)  # pyrefly: ignore[bad-argument-count]

    calls = []
    outside_sub = clib.InterruptionSubscription()
    interruption_scope = interrupt.InterruptionScope()

    def work():
      outside_sub.subscribe(lambda: calls.append('outside'))
      sub1 = clib.InterruptionSubscription()
      sub2 = clib.InterruptionSubscription()
      sub1.subscribe(lambda: calls.append('sub1_v1'))
      sub2.subscribe(lambda: calls.append('sub2'))
      sub1.subscribe(lambda: calls.append('sub1_v2'))  # Overwrites in place.
      interruption_scope.interrupt()
      self.assertEqual(calls, ['sub2', 'sub1_v2'])

      calls.clear()
      sub1.unsubscribe()
      sub1.unsubscribe()  # Idempotent.
      sub1.subscribe(lambda: calls.append('sub1_v3'))
      interruption_scope.interrupt()
      self.assertEqual(calls, ['sub1_v3', 'sub2'])
      return clib.InterruptionSubscription()

    late_sub = interruption_scope.run(work)
    calls.clear()
    late_sub.subscribe(lambda: calls.append('after_run'))
    interruption_scope.interrupt()
    self.assertEqual(calls, [])


if __name__ == '__main__':
  absltest.main()
