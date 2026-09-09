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

from absl.testing import absltest
from arolla import arolla
from arolla.experimental._tasks import sleep as sleep_module
from arolla.experimental._tasks import submit


class SleepTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self.executor = futures.ThreadPoolExecutor()
    self.addCleanup(self.executor.shutdown, wait=True)

  def test_sleep_completes_normally(self):
    t0 = time.monotonic()
    sleep_module.sleep(0.05)
    elapsed = time.monotonic() - t0
    self.assertGreater(elapsed, 0.04)

  def test_sleep_zero(self):
    t0 = time.monotonic()
    sleep_module.sleep(0)
    elapsed = time.monotonic() - t0
    self.assertLess(elapsed, 0.01)

  def test_sleep_negative_raises(self):
    with self.assertRaises(ValueError):
      sleep_module.sleep(-1)

  def test_sleep_wakes_on_cancellation(self):
    started = threading.Event()
    finished = threading.Event()

    def work():
      started.set()
      try:
        sleep_module.sleep(1.0)
      except ValueError:
        finished.set()

    future = submit.submit(self.executor, work)
    self.assertTrue(started.wait(timeout=0.1))
    t0 = time.monotonic()
    future.cancel()
    self.assertTrue(finished.wait(timeout=0.1))
    self.assertLess(time.monotonic() - t0, 0.1)

  def test_sleep_without_cancellation_scope(self):
    t0 = time.monotonic()
    sleep_module.sleep(0.05)
    self.assertGreater(time.monotonic() - t0, 0.04)

  def test_sleep_already_cancelled(self):
    cancellation_context = arolla.abc.CancellationContext()
    cancellation_context.cancel('Boom!')
    t0 = time.monotonic()
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      arolla.abc.run_in_cancellation_context(
          cancellation_context, sleep_module.sleep, 1.0
      )
    self.assertLess(time.monotonic() - t0, 0.01)

  def test_sleep_zero_cancelled(self):
    cancellation_context = arolla.abc.CancellationContext()
    cancellation_context.cancel('Boom!')
    with self.assertRaisesWithLiteralMatch(ValueError, '[CANCELLED] Boom!'):
      arolla.abc.run_in_cancellation_context(
          cancellation_context, sleep_module.sleep, 0
      )


if __name__ == '__main__':
  absltest.main()
