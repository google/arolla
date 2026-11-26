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

from concurrent import futures
import gc
import threading
import time
import weakref

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.experimental._tasks import submit


class SubmitTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.executor = futures.ThreadPoolExecutor(max_workers=8)
    self.addCleanup(self.executor.shutdown, wait=True)

  def test_task_completes(self):
    future = submit.submit(
        self.executor, lambda a, b, *, c: a + b * c, 2, 3, c=4
    )
    self.assertEqual(future.result(timeout=1.0), 14)
    self.assertTrue(future.done())
    self.assertFalse(future.cancelled())
    self.assertIn('state=finished returned int', repr(future))

  def test_task_propagates_exception(self):
    def fail():
      raise ValueError('BOOM!')

    future = submit.submit(self.executor, fail)
    with self.assertRaisesWithLiteralMatch(ValueError, 'BOOM!'):
      future.result(timeout=1.0)
    self.assertTrue(future.done())
    self.assertFalse(future.cancelled())
    self.assertIn('state=finished raised ValueError', repr(future))

  def test_pending_wait_and_timeout(self):
    started = threading.Event()
    release = threading.Event()

    def work():
      started.set()
      release.wait(timeout=1.0)

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))
    self.assertFalse(future.done())
    self.assertFalse(future.cancelled())
    self.assertIn('state=pending', repr(future))
    self.assertFalse(future.wait(timeout=0.0))
    with self.assertRaises(futures.TimeoutError):
      future.result(timeout=0.0)

    release.set()
    self.assertTrue(future.wait(timeout=1.0))
    self.assertTrue(future.done())
    self.assertFalse(future.cancelled())

  def test_cancel_waits_for_worker_to_finish(self):
    started = threading.Event()
    allow_exit = threading.Event()

    def work():
      started.set()
      allow_exit.wait(timeout=1.0)

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))
    future.cancel()
    self.assertTrue(future.cancelled())
    self.assertFalse(future.done())
    self.assertIn('state=cancelling', repr(future))
    with self.assertRaises(futures.TimeoutError):
      future.result(timeout=0.0)

    allow_exit.set()
    with self.assertRaises(futures.CancelledError):
      future.result(timeout=1.0)
    self.assertTrue(future.done())
    self.assertTrue(future.cancelled())
    self.assertIn('state=cancelled', repr(future))

  def test_cancellation_context_exposed_and_direct_cancel(self):
    started = threading.Event()
    cancelled = threading.Event()
    allow_exit = threading.Event()
    caught_error = []

    def work():
      with arolla.abc.subscribe_to_cancellation(cancelled.set):
        started.set()
        cancelled.wait(timeout=1.0)
      try:
        arolla.abc.raise_if_cancelled()
      except ValueError as ex:
        caught_error.append(str(ex))
      allow_exit.wait(timeout=1.0)

    future = submit.submit(self.executor, work)
    self.assertIsInstance(
        future.cancellation_context, arolla.abc.CancellationContext
    )
    self.assertTrue(started.wait(timeout=1.0))

    future.cancellation_context.cancel('custom reason')
    self.assertTrue(future.cancelled())
    self.assertFalse(future.done())

    allow_exit.set()
    with self.assertRaises(futures.CancelledError):
      future.result(timeout=1.0)
    self.assertTrue(future.done())
    self.assertTrue(future.cancelled())
    self.assertEqual(caught_error, ['[CANCELLED] custom reason'])

  def test_interrupt_injects_exception_and_waits_for_worker(self):
    started = threading.Event()
    interrupted = threading.Event()
    allow_exit = threading.Event()

    def work():
      try:
        started.set()
        while True:
          time.sleep(0.005)
      except KeyboardInterrupt:
        interrupted.set()
        allow_exit.wait(timeout=1.0)

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))
    future.interrupt()
    self.assertTrue(interrupted.wait(timeout=1.0))
    self.assertTrue(future.cancelled())
    self.assertFalse(future.done())

    allow_exit.set()
    with self.assertRaises(futures.CancelledError):
      future.result(timeout=1.0)
    self.assertTrue(future.done())

  def test_interrupt_during_startup(self):
    def work():
      while True:
        time.sleep(0.001)

    futures_list = []
    for _ in range(50):
      f = submit.submit(self.executor, work)
      f.interrupt()
      futures_list.append(f)

    for f in futures_list:
      with self.assertRaises(futures.CancelledError):
        f.result(timeout=1.0)

  def test_interrupt_with_invalid_exception_raises_type_error(self):
    future = submit.submit(self.executor, lambda: 42)
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        '`exception_type` must be a BaseException subclass, got'
        ' KeyboardInterrupt()',
    ):
      future.interrupt(
          KeyboardInterrupt()  # pyrefly: ignore[bad-argument-type]
      )
    with self.assertRaisesWithLiteralMatch(
        TypeError,
        "`exception_type` must be a BaseException subclass, got <class 'int'>",
    ):
      future.interrupt(int)  # pyrefly: ignore[bad-argument-type]

  @parameterized.named_parameters(
      ('via_cancel', lambda f: f.cancel()),
      (
          'via_cancellation_context',
          lambda f: f.cancellation_context.cancel('queued cancel'),
      ),
  )
  def test_cancel_before_running(self, cancel_fn):
    barrier = threading.Event()
    blockers = [self.executor.submit(barrier.wait) for _ in range(8)]

    ran = threading.Event()
    future = submit.submit(self.executor, ran.set)
    cancel_fn(future)
    self.assertTrue(future.done())
    self.assertTrue(future.cancelled())

    barrier.set()
    for b in blockers:
      b.result(timeout=1.0)

    with self.assertRaises(futures.CancelledError):
      future.result(timeout=1.0)
    self.assertFalse(ran.is_set())

  def test_cancel_and_interrupt_after_completion_are_noops(self):
    future = submit.submit(self.executor, lambda: 42)
    self.assertEqual(future.result(timeout=1.0), 42)
    self.assertTrue(future.done())
    self.assertFalse(future.cancelled())

    future.cancel()
    future.interrupt()
    self.assertFalse(future.cancelled())
    self.assertFalse(future.cancellation_context.cancelled())
    self.assertEqual(future.result(timeout=0.0), 42)

    future.cancellation_context.cancel()
    self.assertFalse(future.cancelled())
    self.assertEqual(future.result(timeout=0.0), 42)

  def test_completion_unsubscribes_from_cancellation_context(self):
    class _Payload:
      pass

    future = submit.submit(self.executor, _Payload)
    ctx = future.cancellation_context
    payload_ref = weakref.ref(future.result(timeout=1.0))
    del future
    gc.collect()
    self.assertIsNone(payload_ref())
    self.assertFalse(ctx.cancelled())

  def test_cancel_returns_non_bool(self):
    future = submit.submit(self.executor, lambda: 42)
    with self.assertRaises(TypeError):
      bool(future.cancel())

  def test_add_done_callback_on_done(self):
    callback_called = threading.Event()
    received_future = []

    def cb(f):
      received_future.append((f, f.done(), f.result(timeout=0.0)))
      callback_called.set()

    future = submit.submit(self.executor, lambda: 42)
    future.add_done_callback(cb)
    self.assertTrue(callback_called.wait(timeout=1.0))
    self.assertEqual(received_future, [(future, True, 42)])

    callback_called.clear()
    future.add_done_callback(lambda _: callback_called.set())
    self.assertTrue(callback_called.wait(timeout=0.0))

  def test_add_done_callback_on_cancel(self):
    started = threading.Event()
    release = threading.Event()

    def work():
      started.set()
      release.wait(timeout=1.0)

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))

    callback_called = threading.Event()
    callback_saw_cancelled_error = []

    def cb(f):
      try:
        f.result(timeout=0.0)
      except futures.CancelledError:
        callback_saw_cancelled_error.append(f.done() and f.cancelled())
      callback_called.set()

    future.add_done_callback(cb)
    future.cancel()

    self.assertFalse(callback_called.is_set())

    release.set()
    self.assertTrue(callback_called.wait(timeout=1.0))
    self.assertEqual(callback_saw_cancelled_error, [True])

    callback_called.clear()
    future.add_done_callback(lambda _: callback_called.set())
    self.assertTrue(callback_called.wait(timeout=0.0))

  def test_exception_in_callback(self):
    release = threading.Event()
    future = submit.submit(self.executor, lambda: (release.wait(), 42)[1])

    def bad_callback(_):
      raise RuntimeError('BOOM!')

    future.add_done_callback(bad_callback)

    good_callback_called = threading.Event()
    future.add_done_callback(lambda _: good_callback_called.set())
    release.set()
    self.assertTrue(good_callback_called.wait(timeout=1.0))
    self.assertEqual(future.result(timeout=1.0), 42)

  def test_executor_shutdown_cancel(self):
    executor = futures.ThreadPoolExecutor(max_workers=1)
    release = threading.Event()
    executor.submit(release.wait)

    future = submit.submit(executor, lambda: 42)
    executor.shutdown(wait=False, cancel_futures=True)
    with self.assertRaises(futures.CancelledError):
      future.result(timeout=1.0)
    self.assertTrue(future.done())
    self.assertTrue(future.cancelled())

    release.set()

  def test_stress(self):
    n = 120

    def wait_for_arolla_cancellation():
      ev = threading.Event()
      with arolla.abc.subscribe_to_cancellation(ev.set):
        ev.wait(timeout=1.0)

    def wait_for_exception_injection():
      while True:
        time.sleep(0.001)

    def do_computation():
      return sum(range(1000))

    tasks = (
        wait_for_arolla_cancellation,
        wait_for_exception_injection,
        do_computation,
    )
    futures_list = [
        submit.submit(self.executor, tasks[i % 3]) for i in range(n)
    ]

    time.sleep(0.02)
    for f in futures_list:
      f.cancel()
    time.sleep(0.02)
    for f in futures_list:
      f.interrupt()

    for f in futures_list:
      try:
        f.result(timeout=5.0)
      except futures.CancelledError:
        pass
      self.assertTrue(f.done())


if __name__ == '__main__':
  absltest.main()
