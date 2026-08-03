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
import sys
import threading
import time

from absl.testing import absltest
from arolla import arolla
from arolla.experimental._tasks import callbacks
from arolla.experimental._tasks import clib
from arolla.experimental._tasks import testing_clib


class PythonCallbackBridgeTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self._bridge = clib.PythonCallbackBridge()

  def tearDown(self):
    self._bridge.close()
    super().tearDown()

  def test_basics(self):
    called = threading.Event()
    testing_clib.schedule_callback(self._bridge, called.set)
    self.assertTrue(called.wait(timeout=1.0))

  def test_duplicate_callbacks(self):
    count = 0
    called = threading.Event()

    def cb():
      nonlocal count
      count += 1
      if count == 3:
        called.set()

    testing_clib.schedule_callback(self._bridge, cb, delay_seconds=0.01)
    testing_clib.schedule_callback(self._bridge, cb, delay_seconds=0.01)
    testing_clib.schedule_callback(self._bridge, cb, delay_seconds=0.01)

    self.assertTrue(called.wait(timeout=1.0))
    self.assertEqual(count, 3)

  def test_callback_exception(self):
    called = threading.Event()

    def cb():
      called.set()
      raise SystemExit("Boom!")

    testing_clib.schedule_callback(self._bridge, cb)
    self.assertTrue(called.wait(timeout=1.0))
    called_again = threading.Event()
    testing_clib.schedule_callback(self._bridge, called_again.set)
    self.assertTrue(called_again.wait(timeout=1.0))

  def test_callback_refcount(self):
    n = 3
    count = 0

    def cb():
      nonlocal count
      count += 1
      raise SystemExit("Boom!")

    gc.collect()
    initial_refcount = sys.getrefcount(cb)

    for _ in range(n):
      testing_clib.schedule_callback(self._bridge, cb, delay_seconds=0.01)
      testing_clib.schedule_callback(
          self._bridge, cb, do_call=False, delay_seconds=0.01
      )

    for _ in range(10):
      gc.collect()
      if sys.getrefcount(cb) == initial_refcount:
        break
      time.sleep(0.01)
    gc.collect()
    self.assertEqual(sys.getrefcount(cb), initial_refcount)

    self.assertEqual(count, n)

  def test_close(self):
    self._bridge.close()

    called = False

    def cb():
      nonlocal called
      called = True

    gc.collect()
    initial_refcount = sys.getrefcount(cb)

    testing_clib.schedule_callback(self._bridge, cb)
    gc.collect()
    self.assertEqual(sys.getrefcount(cb), initial_refcount)
    self.assertFalse(called)

  def test_gc_stop_thread(self):
    bridge = clib.PythonCallbackBridge()
    thread = None
    called_1 = threading.Event()

    def cb():
      nonlocal thread
      thread = threading.current_thread()
      called_1.set()

    testing_clib.schedule_callback(bridge, cb)
    self.assertTrue(called_1.wait(timeout=1.0))
    assert thread
    self.assertTrue(thread.is_alive())

    called_2 = threading.Event()
    testing_clib.schedule_callback(bridge, called_2.set, delay_seconds=0.01)
    del bridge
    gc.collect()
    thread.join(timeout=1.0)
    self.assertFalse(thread.is_alive())
    self.assertFalse(called_2.wait(timeout=0.1))


class CancellationSubscriptionTest(absltest.TestCase):

  def setUp(self):
    super().setUp()
    self._bridge = clib.PythonCallbackBridge()

  def tearDown(self):
    self._bridge.close()
    super().tearDown()

  def test_cancellation_subscription(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()
    clib.subscribe_to_cancellation(
        self._bridge, called.set, cancellation_context
    )
    self.assertFalse(called.is_set())
    cancellation_context.cancel()
    self.assertTrue(called.wait(timeout=1.0))

  def test_cancellation_subscription_default_context(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()

    def target():
      clib.subscribe_to_cancellation(self._bridge, called.set, None)
      self.assertFalse(called.is_set())
      self.assertFalse(arolla.abc.cancelled())
      cancellation_context.cancel()
      self.assertTrue(called.wait(timeout=1.0))

    arolla.abc.run_in_cancellation_context(cancellation_context, target)

  def test_cancellation_subscription_invalid_type(self):
    with self.assertRaisesWithLiteralMatch(
        TypeError, "expected arolla.abc.CancellationContext, got int"
    ):
      clib.subscribe_to_cancellation(self._bridge, lambda: None, 123)  # type: ignore

  def test_cancellation_subscription_no_context(self):
    def target():
      with self.assertRaisesWithLiteralMatch(
          RuntimeError, "current thread has no active cancellation context"
      ):
        clib.subscribe_to_cancellation(self._bridge, lambda: None, None)

    thread = threading.Thread(target=target)
    thread.start()
    thread.join()


class CallbacksTest(absltest.TestCase):

  def test_default_callback_bridge(self):
    bridge = callbacks.default_callback_bridge()
    self.assertIsInstance(bridge, clib.PythonCallbackBridge)
    self.assertIs(bridge, callbacks.default_callback_bridge())

  def test_subscribe_to_cancellation(self):
    called = threading.Event()
    cancellation_context = arolla.abc.CancellationContext()
    callbacks.subscribe_to_cancellation(
        called.set, cancellation_context=cancellation_context
    )
    self.assertFalse(called.is_set())
    cancellation_context.cancel()
    self.assertTrue(called.wait(timeout=1.0))


if __name__ == "__main__":
  absltest.main()
