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
import threading
import time
from unittest import mock

from absl.testing import absltest
from arolla import arolla
from arolla.experimental._tasks import submit


@mock.patch.object(submit._State, 'FORCE_CANCELLATION_DELAY_SECONDS', 0.05)
class SubmitTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.executor = futures.ThreadPoolExecutor(max_workers=8)
    self.addCleanup(self.executor.shutdown, wait=True)

  def test_task_completes(self):
    def work(a, b, *, c):
      return a + b * c

    future = submit.submit(self.executor, work, 2, 3, c=4)
    self.assertEqual(future.result(), 14)

  def test_task_propagates_exception(self):
    def fail():
      raise ValueError('BOOM!')

    future = submit.submit(self.executor, fail)
    with self.assertRaisesWithLiteralMatch(ValueError, 'BOOM!'):
      future.result(timeout=1.0)

  def test_done_returns_false_while_pending(self):
    started = threading.Event()
    release = threading.Event()

    def work():
      started.set()
      release.wait(1.0)

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))
    self.assertFalse(future.done())
    release.set()
    future.result(timeout=1.0)
    self.assertTrue(future.done())

  def test_cancellation_context(self):
    started = threading.Event()
    cancelled = threading.Event()

    def work():
      started.set()
      while not arolla.abc.cancelled():
        time.sleep(0.01)
      cancelled.set()

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))
    future.cancel()
    with self.assertRaises(futures.CancelledError):
      future.result(timeout=0.0)
    self.assertTrue(cancelled.wait(timeout=1.0))

  def test_exception_injection(self):
    started = threading.Event()
    cancelled = threading.Event()

    def work():
      try:
        started.set()
        while True:
          time.sleep(0.01)
      except KeyboardInterrupt:
        cancelled.set()

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))
    future.cancel()
    with self.assertRaises(futures.CancelledError):
      future.result(timeout=0.0)
    self.assertTrue(cancelled.wait(timeout=1.0))

  def test_cancel_before_running(self):
    # Fill the executor so our task is queued.
    barrier = threading.Event()
    blockers = []
    for _ in range(8):
      blockers.append(self.executor.submit(barrier.wait))

    future = submit.submit(self.executor, lambda: 42)
    future.cancel()  # Cancel while still INITIAL.

    barrier.set()
    for b in blockers:
      b.result(timeout=1.0)

    with self.assertRaises(futures.CancelledError):
      future.result(timeout=1.0)

  def test_cancel_returns_non_bool(self):
    future = submit.submit(self.executor, lambda: 42)
    non_bool = future.cancel()
    with self.assertRaises(TypeError):
      bool(non_bool)

  def test_repr_pending_finished(self):
    started = threading.Event()
    release = threading.Event()

    def work():
      started.set()
      release.wait()
      return 42

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))
    self.assertIn('state=pending', repr(future))
    release.set()
    future.result(timeout=1.0)
    self.assertIn('state=finished returned int', repr(future))

  def test_repr_raised(self):
    def fail():
      raise ValueError('BOOM!')

    future = submit.submit(self.executor, fail)
    with self.assertRaisesWithLiteralMatch(ValueError, 'BOOM!'):
      future.result(timeout=1.0)
    self.assertIn('state=finished raised ValueError', repr(future))

  def test_add_done_callback_on_done(self):
    callback_called = threading.Event()
    received_future = []

    def cb(f):
      self.assertIs(f.result(timeout=0.0), 42)
      received_future.append(f)
      callback_called.set()

    future = submit.submit(self.executor, lambda: 42)
    future.add_done_callback(cb)
    self.assertTrue(callback_called.wait(timeout=1.0))
    self.assertIs(received_future[0], future)

    callback_called.clear()
    future.add_done_callback(lambda _: callback_called.set())
    self.assertTrue(callback_called.wait(timeout=0.0))

  def test_add_done_callback_on_cancel(self):
    release = threading.Event()
    future = submit.submit(self.executor, release.wait, timeout=1.0)

    callback_called = threading.Event()

    def cb(f):
      with self.assertRaises(futures.CancelledError):
        f.result(timeout=0.0)
      callback_called.set()

    future.add_done_callback(cb)
    future.cancel()
    self.assertTrue(callback_called.wait(timeout=1.0))

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
    # The future result is not affected by the callback exception.
    self.assertEqual(future.result(timeout=1.0), 42)

  def test_result_timeout(self):
    started = threading.Event()
    release = threading.Event()

    def work():
      started.set()
      release.wait(1.0)

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=1.0))
    with self.assertRaises(futures.TimeoutError):
      future.result(timeout=0.01)
    release.set()

  def test_executor_shutdown_cancel(self):
    executor = futures.ThreadPoolExecutor(max_workers=1)
    release = threading.Event()
    executor.submit(release.wait)

    # Submit a task that will be queued (not yet running).
    future = submit.submit(executor, lambda: 42)
    executor.shutdown(wait=False, cancel_futures=True)
    with self.assertRaises(futures.CancelledError):
      future.result(timeout=1.0)

    release.set()

  def test_stress(self):
    n = 400
    m = 5000

    def wait_for_arolla_cancellation():
      while not arolla.abc.cancelled():
        time.sleep(0.001)

    def wait_for_exception_injection():
      while True:
        time.sleep(0.001)

    def do_computation():
      x = 0
      for i in range(m):
        x += i
      return x

    futures_list = []
    for i in range(n):
      if (i * 5) % 7 < 2:
        futures_list.append(
            submit.submit(self.executor, wait_for_arolla_cancellation)
        )
      elif (i * 5) % 7 < 4:
        futures_list.append(
            submit.submit(self.executor, wait_for_exception_injection)
        )
      else:
        futures_list.append(submit.submit(self.executor, do_computation))

    time.sleep(0.1)
    for f in futures_list:
      f.cancel()

    for f in futures_list:
      try:
        f.result()
      except futures.CancelledError:
        pass


if __name__ == '__main__':
  absltest.main()
