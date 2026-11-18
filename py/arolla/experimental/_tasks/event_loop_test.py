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

import re
import threading
import time

from absl.testing import absltest
from absl.testing import parameterized
from arolla.experimental._tasks import event_loop


class ScheduleCallbackAfterTest(parameterized.TestCase):

  @parameterized.parameters(0.0, 0.02, 0.03, 0.1)
  def test_basics(self, delay_seconds):
    results = []
    done = threading.Event()
    start = time.monotonic()
    elapsed = []

    def cb(a, b, *, c):
      results.append(a + b * c)
      elapsed.append(time.monotonic() - start)
      done.set()

    event_loop.schedule_callback_after(delay_seconds, cb, 1, 2, c=3)
    self.assertTrue(done.wait(timeout=1.0))
    self.assertEqual(results, [7])
    self.assertGreaterEqual(elapsed[0], delay_seconds)
    self.assertLessEqual(elapsed[0], delay_seconds + 0.05)  # remove if flaky

  def test_negative_delay(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, '`seconds` must be non-negative, got -0.1'
    ):
      event_loop.schedule_callback_after(-0.1, lambda: None)
    with self.assertRaisesWithLiteralMatch(
        ValueError, '`seconds` must be non-negative, got nan'
    ):
      event_loop.schedule_callback_after(float('nan'), lambda: None)

  @parameterized.parameters(ValueError, KeyboardInterrupt, SystemExit)
  def test_unhandled_exception(self, exc_type):
    started = threading.Event()
    done = threading.Event()

    def fail():
      started.set()
      raise exc_type('Boom!')

    with self.assertWarnsRegex(
        RuntimeWarning,
        re.escape(
            'arolla_tasks.schedule_callback_after: '
            f'unhandled exception in callback {fail!r}:'
            f' {exc_type("Boom!")!r}'
        ),
    ):
      event_loop.schedule_callback_after(0.0, fail)
      self.assertTrue(started.wait(timeout=1.0))
      event_loop.schedule_callback_after(0.0, done.set)
      self.assertTrue(done.wait(timeout=1.0))

  def test_non_callable(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, '`callback` must be callable, got int'
    ):
      event_loop.schedule_callback_after(0.0, 123)  # pyrefly: ignore[bad-argument-type]


if __name__ == '__main__':
  absltest.main()
