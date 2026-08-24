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

import threading
import time

from absl.testing import absltest
from arolla import arolla
from arolla.experimental._tasks import locks


class LockTest(absltest.TestCase):

  def test_basic_acquire_release(self):
    lock = locks.Lock()
    lock.acquire()
    lock.release()

  def test_context_manager(self):
    lock = locks.Lock()
    with lock:
      pass

  def test_non_blocking_acquire(self):
    lock = locks.Lock()
    self.assertTrue(lock.acquire(blocking=False))
    self.assertFalse(lock.acquire(blocking=False))
    lock.release()
    self.assertTrue(lock.acquire(blocking=False))
    lock.release()

  def test_mutual_exclusion(self):
    n = 100
    lock = locks.Lock()

    barrier = threading.Barrier(n)
    next_a = 0
    next_b = 0

    def worker() -> None:
      nonlocal next_a, next_b
      barrier.wait()
      with lock:
        a = next_a
        b = next_b
        time.sleep(0.001)
        next_a = a + 1
        next_b = b + 1
      self.assertEqual(a, b)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
      t.start()
    for t in threads:
      t.join()
    self.assertEqual(next_a, n)
    self.assertEqual(next_b, n)

  @arolla.abc.add_default_cancellation_context
  def test_acquire_if_cancelled(self):
    lock = locks.Lock()
    cancellation_context = arolla.abc.current_cancellation_context()
    self.assertIsNotNone(cancellation_context)
    cancellation_context.cancel('Boom!')
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      lock.acquire(blocking=False)
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      lock.acquire(blocking=True)

  def test_acquire_if_cancelled_while_waiting(self):
    lock = locks.Lock()
    lock.acquire()
    started = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()

    def worker():
      started.wait(1.0)
      time.sleep(0.05)
      cancellation_context.cancel('Boom!')

    threading.Thread(target=worker, daemon=True).start()

    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      started.set()
      arolla.abc.run_in_cancellation_context(cancellation_context, lock.acquire)

  def test_release_unlocked_lock(self):
    lock = locks.Lock()
    with self.assertRaisesWithLiteralMatch(
        RuntimeError, 'release unlocked lock'
    ):
      lock.release()

  @arolla.abc.add_default_cancellation_context
  def test_release_after_cancellation(self):
    lock = locks.Lock()
    with lock:
      cancellation_context = arolla.abc.current_cancellation_context()
      self.assertIsNotNone(cancellation_context)
      cancellation_context.cancel('Boom!')
    self.assertTrue(
        arolla.abc.run_in_cancellation_context(  # override "current" context
            None, lock.acquire, blocking=False
        )
    )

  def test_context_manager_exception_propagation(self):
    lock = locks.Lock()
    with self.assertRaisesRegex(RuntimeError, 'Boom!'):
      with lock:
        raise RuntimeError('Boom!')
    self.assertTrue(lock.acquire(blocking=False))


if __name__ == '__main__':
  absltest.main()
