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


if __name__ == '__main__':
  absltest.main()
