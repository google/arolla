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
import re
import sys
import threading
import time

from absl.testing import absltest
from arolla import arolla
from arolla.experimental._tasks import callbacks
from arolla.experimental._tasks import clib
from arolla.experimental._tasks import testing_clib


class ScheduleCallbackTest(absltest.TestCase):

  def test_basics(self):
    called = threading.Event()
    testing_clib.schedule_callback(called.set)
    self.assertTrue(called.wait(timeout=1.0))

  def test_duplicate_callbacks(self):
    count = 0
    called = threading.Event()

    def cb():
      nonlocal count
      count += 1
      if count == 3:
        called.set()

    testing_clib.schedule_callback(cb, delay_seconds=0.01)
    testing_clib.schedule_callback(cb, delay_seconds=0.01)
    testing_clib.schedule_callback(cb, delay_seconds=0.01)

    self.assertTrue(called.wait(timeout=1.0))
    self.assertEqual(count, 3)

  def test_callback_exception(self):
    called = threading.Event()

    def cb():
      called.set()
      raise SystemExit('Boom!')

    testing_clib.schedule_callback(cb)
    self.assertTrue(called.wait(timeout=1.0))
    called_again = threading.Event()
    testing_clib.schedule_callback(called_again.set)
    self.assertTrue(called_again.wait(timeout=1.0))

  def test_callback_refcount(self):
    n = 3
    count = 0

    def cb():
      nonlocal count
      count += 1
      raise SystemExit('Boom!')

    gc.collect()
    initial_refcount = sys.getrefcount(cb)

    for _ in range(n):
      testing_clib.schedule_callback(cb, delay_seconds=0.01)
      testing_clib.schedule_callback(cb, do_call=False, delay_seconds=0.01)

    for _ in range(10):
      gc.collect()
      if sys.getrefcount(cb) == initial_refcount:
        break
      time.sleep(0.01)
    gc.collect()
    self.assertEqual(sys.getrefcount(cb), initial_refcount)

    self.assertEqual(count, n)


class CancellationSubscriptionTest(absltest.TestCase):

  def test_cancellation_subscription(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()
    clib.subscribe_to_cancellation(called.set, cancellation_context)
    self.assertFalse(called.is_set())
    cancellation_context.cancel()
    self.assertTrue(called.wait(timeout=1.0))

  def test_default_cancellation_context(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()

    def target():
      clib.subscribe_to_cancellation(called.set)
      self.assertFalse(called.is_set())
      self.assertFalse(arolla.abc.cancelled())
      cancellation_context.cancel()
      self.assertTrue(called.wait(timeout=1.0))

    arolla.abc.run_in_cancellation_context(cancellation_context, target)

  def test_no_cancellation_context_error(self):
    def target():
      with self.assertRaisesWithLiteralMatch(
          RuntimeError, 'current thread has no active cancellation context'
      ):
        clib.subscribe_to_cancellation(lambda: None)

    thread = threading.Thread(target=target)
    thread.start()
    thread.join()

  def test_cancellation_context_type_error(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected arolla.abc.CancellationContext, got int'
    ):
      clib.subscribe_to_cancellation(lambda: None, 123)  # pyrefly: ignore[bad-argument-type]


class CallbacksTest(absltest.TestCase):

  def test_subscribe_to_cancellation(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()
    callbacks.subscribe_to_cancellation(
        called.set, cancellation_context=cancellation_context
    )
    self.assertFalse(called.is_set())
    cancellation_context.cancel()
    self.assertTrue(called.wait(timeout=1.0))

  def test_subscribe_context_manager_cancelled_inside(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()
    with callbacks.subscribe_to_cancellation(
        called.set, cancellation_context=cancellation_context
    ):
      cancellation_context.cancel()
    self.assertTrue(called.wait(timeout=0.1))

  def test_subscribe_context_manager_cancelled_outside(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()
    with callbacks.subscribe_to_cancellation(
        called.set, cancellation_context=cancellation_context
    ):
      pass
    cancellation_context.cancel()
    self.assertFalse(called.wait(timeout=0.1))

  def test_subscribe_already_cancelled(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()
    cancellation_context.cancel()
    # Callback should be executed synchronously upon subscription.
    callbacks.subscribe_to_cancellation(
        called.set, cancellation_context=cancellation_context
    )
    self.assertTrue(called.is_set())

  def test_subscribe_already_cancelled_callback_raises(self):
    cancellation_context = arolla.abc.CancellationContext()
    cancellation_context.cancel()

    def bad_cb():
      raise RuntimeError('Boom!')

    with self.assertWarnsRegex(
        RuntimeWarning, re.escape('unhandled exception in callback')
    ):
      callbacks.subscribe_to_cancellation(
          bad_cb, cancellation_context=cancellation_context
      )


if __name__ == '__main__':
  absltest.main()
