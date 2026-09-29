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

import atexit
import gc
import os
import subprocess
import sys
import threading
import time
import weakref

from absl.testing import absltest

# Add a list of actions to run *after* PyObjectBridge::Stop().
atexit_prepend = []
atexit.register(lambda: [fn() for fn in atexit_prepend])

from arolla.py_utils import testing_clib  # pylint: disable=g-import-not-at-top


class HolderTest(absltest.TestCase):

  def test_basic(self):
    obj = object()
    holder = testing_clib.PyObjectHolder()
    holder.set(obj)
    self.assertIs(holder.get(), obj)

  def test_move_lifecycle(self):
    obj = object()
    initial_refcount = sys.getrefcount(obj)
    holder1 = testing_clib.PyObjectHolder()
    holder1.set(obj)
    gc.collect()
    self.assertEqual(sys.getrefcount(obj), initial_refcount + 1)

    holder2 = testing_clib.PyObjectHolder()
    holder2.move_from(holder1)
    self.assertIsNone(holder1.get())
    self.assertIsNotNone(holder2.get())
    gc.collect()
    self.assertEqual(sys.getrefcount(obj), initial_refcount + 1)

    holder2.set(None)
    gc.collect()
    self.assertEqual(sys.getrefcount(obj), initial_refcount)

  def test_self_move(self):
    obj = object()
    initial_refcount = sys.getrefcount(obj)
    holder = testing_clib.PyObjectHolder()
    holder.set(obj)
    holder.move_from(holder)
    holder.set(None)
    gc.collect()
    self.assertEqual(sys.getrefcount(obj), initial_refcount)

  def test_decref(self):
    with self.subTest('inline'):
      obj = object()
      initial_refcount = sys.getrefcount(obj)
      holder = testing_clib.PyObjectHolder()
      holder.set(obj)
      gc.collect()
      self.assertEqual(sys.getrefcount(obj), initial_refcount + 1)
      holder.set(None)
      gc.collect()
      self.assertEqual(sys.getrefcount(obj), initial_refcount)
    with self.subTest('post'):
      obj = object()
      initial_refcount = sys.getrefcount(obj)
      holder = testing_clib.PyObjectHolder()
      holder.set(obj)
      gc.collect()
      self.assertEqual(sys.getrefcount(obj), initial_refcount + 1)
      holder.post_decref()
      gc.collect()
      self.assertGreaterEqual(sys.getrefcount(obj), initial_refcount)
      time.sleep(0.1)
      gc.collect()
      self.assertEqual(sys.getrefcount(obj), initial_refcount)

  def test_call(self):
    with self.subTest('inline'):
      event = threading.Event()
      event_set = lambda: event.set()  # pylint: disable=unnecessary-lambda
      initial_refcount = sys.getrefcount(event_set)
      holder = testing_clib.PyObjectHolder()
      holder.set(event_set)
      gc.collect()
      self.assertEqual(sys.getrefcount(event_set), initial_refcount + 1)
      self.assertIsNotNone(holder.get())
      holder.call()
      self.assertIsNone(holder.get())
      gc.collect()
      self.assertEqual(sys.getrefcount(event_set), initial_refcount)
      self.assertTrue(event.is_set())
    with self.subTest('post'):
      event = threading.Event()
      event_set = lambda: event.set()  # pylint: disable=unnecessary-lambda
      initial_refcount = sys.getrefcount(event_set)
      holder = testing_clib.PyObjectHolder()
      holder.set(event_set)
      gc.collect()
      self.assertEqual(sys.getrefcount(event_set), initial_refcount + 1)
      self.assertIsNotNone(holder.get())
      holder.post_call()
      self.assertIsNone(holder.get())
      gc.collect()
      self.assertGreaterEqual(sys.getrefcount(event_set), initial_refcount)
      self.assertTrue(event.wait(timeout=1.0))
      gc.collect()
      self.assertEqual(sys.getrefcount(event_set), initial_refcount)

  def test_deep_stack_on_worker_thread(self):
    event = threading.Event()

    def fn(n=500):
      if n <= 0:
        event.set()
        return 0
      return n + fn(n - 1)

    holder = testing_clib.PyObjectHolder()
    holder.set(fn)
    holder.post_call()
    self.assertTrue(event.wait(timeout=1.0))

  def test_exception_does_not_crash(self):
    def bad_callback():
      raise RuntimeError('test error')

    with self.subTest('inline'):
      holder = testing_clib.PyObjectHolder()
      holder.set(bad_callback)
      with self.assertWarnsRegex(
          RuntimeWarning,
          'arolla::python::PyObjectBridge::RunAction: '
          'clearing stale Python exception',
      ):
        holder.call()

    with self.subTest('post'):
      holder = testing_clib.PyObjectHolder()
      holder.set(bad_callback)
      with self.assertWarnsRegex(
          RuntimeWarning,
          'arolla::python::PyObjectBridge::RunAction: '
          'clearing stale Python exception',
      ):
        holder.post_call()
        time.sleep(0.1)

  def test_multiple_concurrent_callbacks(self):
    count = 0
    lock = threading.Lock()
    done_event = threading.Event()
    n = 50

    def increment():
      nonlocal count
      with lock:
        count += 1
        if count == n:
          done_event.set()

    holder = testing_clib.PyObjectHolder()
    for _ in range(n):
      holder.set(increment)
      holder.post_call()
    self.assertTrue(done_event.wait(timeout=1.0))
    self.assertEqual(count, n)

  def test_init_py_object_bridge(self):
    testing_clib.init_py_object_bridge()  # No exception.

  def test_child_process(self):
    env = os.environ.copy()
    env['PYTHONPATH'] = os.pathsep.join(sys.path + [env.get('PYTHONPATH', '')])
    cmd = (
        [sys.executable, __file__]
        if sys.executable and sys.executable != sys.argv[0]
        else [sys.argv[0]]
    )
    proc = subprocess.run(
        cmd + ['--run_child_process_test'],
        capture_output=True,
        env=env,
        text=True,
        check=False,
        timeout=5.0,
    )
    self.assertEqual(proc.returncode, 0, msg=proc.stderr)
    self.assertEqual(proc.stderr, '')


def _find_worker_thread() -> threading.Thread:
  result: threading.Thread | None = None
  event = threading.Event()

  def set_event():
    nonlocal result
    result = threading.current_thread()
    event.set()

  holder = testing_clib.PyObjectHolder()
  holder.set(set_event)
  holder.post_call()
  event.wait(timeout=1.0)
  assert result is not None, 'Worker thread not found'
  return result


def _run_child_process_test() -> None:
  worker_thread = _find_worker_thread()
  pre_stop_event = threading.Event()
  pre_stop_obj = set()
  pre_stop_ref = weakref.ref(pre_stop_obj)

  def _verify_after_bridge_stop():
    # 1. Verify the worker thread has stopped and joined.
    assert not worker_thread.is_alive(), 'Worker thread still alive'

    # 2. Verify items queued before `Stop()` were drained before `Stop()`
    # finished.
    gc.collect()
    assert pre_stop_event.is_set(), 'Pending action was not drained on Stop()'
    assert pre_stop_ref() is None, 'Pending decref was not drained on Stop()'

    # 3. Verify behaviour AFTER `Stop()` has completed: non-inline deallocation
    # (without GIL) does not crash and intentionally leaks the reference since
    # the worker is stopped.
    holder = testing_clib.PyObjectHolder()
    obj_thread = object()
    initial_refcount = sys.getrefcount(obj_thread)
    holder.set(obj_thread)
    holder.post_decref()
    time.sleep(0.1)
    gc.collect()
    assert sys.getrefcount(obj_thread) == initial_refcount + 1

    post_stop_thread_event = threading.Event()
    holder.set(post_stop_thread_event.set)
    holder.post_call()
    assert not post_stop_thread_event.wait(timeout=0.02)

  atexit_prepend.append(_verify_after_bridge_stop)

  pre_stop_call_holder = testing_clib.PyObjectHolder()
  pre_stop_call_holder.set(pre_stop_event.set)
  pre_stop_decref_holder = testing_clib.PyObjectHolder()
  pre_stop_decref_holder.set(pre_stop_obj)
  del pre_stop_obj

  def _enqueue_before_bridge_stop():
    pre_stop_call_holder.post_call()
    pre_stop_decref_holder.post_decref()

  atexit.register(_enqueue_before_bridge_stop)
  sys.exit(0)


if __name__ == '__main__':
  if '--run_child_process_test' in sys.argv:
    _run_child_process_test()
  else:
    absltest.main()
