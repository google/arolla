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

"""(Internal) Utility for starting a cancellable task on an executor."""

from concurrent import futures
import threading
from typing import Any, Callable, Self

from arolla import arolla
from arolla.experimental._tasks import interrupt


class _NonBool:
  """A utility class that always raises an error on bool(self)."""

  __slots__ = ()

  def __bool__(self):
    raise TypeError('Truth value of this object is ambiguous.')


_NON_BOOL = _NonBool()


class _Result[T]:
  """A utility structure that stores either an exception or a value."""

  __slots__ = ('exception', 'value')

  exception: BaseException
  value: T

  def is_exception(self) -> bool:
    return hasattr(self, 'exception')

  def is_value(self) -> bool:
    return hasattr(self, 'value')


class _State[T]:
  """The internal state of a TaskFuture.

  A thread-safe state machine managing the lifecycle of a task submitted to
  a thread pool executor. All state transitions are guarded by `self.lock`.
  The machine supports cooperative cancellation via
  `arolla.abc.CancellationContext` and forced interruption via
  `arolla.experimental.tasks.InterruptionScope`.

  States:
    INITIAL: The task has been submitted but the worker thread has not yet
      started executing the task function.
    RUNNING: The worker thread is actively executing the task function.
    CANCELLING: Cancellation was requested while the task was RUNNING. The
      cancellation context has been signaled, and the worker thread is still
      executing the task function.
    DONE: Terminal state. The computation has completed (successfully, with an
      exception, or via cancellation).

  Result and callback semantics:
    - `result_ready` is set when the state transitions to DONE (either when
      the worker thread finishes executing the task function, or immediately if
      cancelled while still in INITIAL before the worker started).
    - Callbacks are fired exactly once, upon transitioning to DONE. They are
      always executed outside `self.lock` to prevent deadlocks.

  Main state transition paths:
    INITIAL -> RUNNING -> DONE:
      The computation starts and completes.
    INITIAL -> DONE:
      The computation is cancelled before the worker thread started execution.
    INITIAL -> RUNNING -> CANCELLING -> DONE:
      The computation is cancelled (or interrupted) while running, and
      transitions to DONE once the worker thread stops executing the task
      function.
  """

  STATE_INITIAL = 'INITIAL'
  STATE_RUNNING = 'RUNNING'
  STATE_CANCELLING = 'CANCELLING'
  STATE_DONE = 'DONE'  # terminal state

  __slots__ = (
      # go/keep-sorted start
      'cancellation_context',
      'cancellation_subscription',
      'done_callbacks',
      'internal_future',
      'interruption_scope',
      'lock',
      'result',
      'result_ready',
      'state',
      # go/keep-sorted end
  )

  # go/keep-sorted start
  cancellation_context: arolla.abc.CancellationContext
  cancellation_subscription: arolla.abc.CancellationContext.Subscription
  done_callbacks: list[Callable[[], Any]]
  internal_future: futures.Future[None]
  interruption_scope: interrupt.InterruptionScope
  lock: threading.Lock
  result: _Result[T] | None
  result_ready: threading.Event
  state: str
  # go/keep-sorted end

  def __init__(self) -> None:
    # go/keep-sorted start
    self.cancellation_context = arolla.abc.CancellationContext()
    self.cancellation_subscription = self.cancellation_context.subscribe(
        self.on_cancel
    )
    self.done_callbacks = []
    self.interruption_scope = interrupt.InterruptionScope()
    self.lock = threading.Lock()
    self.result = None
    self.result_ready = threading.Event()
    self.state = self.STATE_INITIAL
    # go/keep-sorted end

  def is_cancelled(self) -> bool:
    """Returns True if cancellation was requested before the task finished."""
    return self.state is self.STATE_CANCELLING or (
        self.state is self.STATE_DONE and self.result is None
    )

  def on_running(self) -> bool:
    """Handles the start of `task()`.

    Called by the worker thread.

    State transitions:
      INITIAL -> RUNNING:
        Marks the task as actively running.
      INITIAL -> INITIAL (no-op):
        `cancellation_context` was already cancelled before `on_running()`
        acquired `self.lock`. `on_finishing()` (or `on_cancel()`) will
        transition to DONE.
      DONE -> DONE (no-op):
        Cancellation occurred before `task()` could report it was actually
        running.

    Returns:
      True if the state transitioned to RUNNING; otherwise, False.
    """
    with self.lock:
      assert self.state in (self.STATE_INITIAL, self.STATE_DONE)
      if (
          self.state is self.STATE_INITIAL
          and not self.cancellation_context.cancelled()
      ):
        self.state = self.STATE_RUNNING
        return True
      return False

  def on_internal_future_done(self, f: futures.Future[None]) -> None:
    """Handles cancellation of the internal `concurrent.futures.Future`.

    Invoked when the internal future completes. Only acts when the future
    was cancelled (e.g. the executor shut down, or `on_cancel()` cancelled
    the future before the worker started).

    Args:
      f: The internal future.
    """
    if f.cancelled():
      self.on_cancel()

  def on_cancel(self) -> None:
    """Handles a cancellation request from the consumer or cancellation context.

    State transitions:
      INITIAL -> DONE:
        `task()` hasn't started yet.
      RUNNING -> CANCELLING:
        `task()` started but hasn't finished yet. Signals the cancellation
        context; the transition to DONE happens in `on_finishing()` once the
        worker stops.
      All other states remain unchanged: the task is either already done or
        already cancelling.
    """
    done_callbacks = []
    should_cancel_future = False
    with self.lock:
      if self.state is self.STATE_INITIAL:
        self.state = self.STATE_DONE
        self.result_ready.set()
        done_callbacks, self.done_callbacks = self.done_callbacks, []
        should_cancel_future = True
      elif self.state is self.STATE_RUNNING:
        self.state = self.STATE_CANCELLING
        should_cancel_future = True
      should_cancel_context = self.is_cancelled()
    if should_cancel_future:
      self.cancellation_subscription.unsubscribe()
      self.internal_future.cancel()
    if should_cancel_context:
      self.cancellation_context.cancel()
    self._run_callbacks(done_callbacks)

  def on_interrupt(self, exception_type: type[BaseException]) -> None:
    """Handles a forced interruption request.

    State transitions:
      INITIAL -> DONE:
        `task()` hasn't started yet.
      RUNNING | CANCELLING -> CANCELLING:
        Signals the cancellation context (if not already cancelled) and
        delegates interruption to `self.interruption_scope`.
      DONE -> DONE (no-op):
        The worker already finished.

    Args:
      exception_type: The `BaseException` subclass to inject into the worker
        thread.
    """
    self.on_cancel()
    self.interruption_scope.interrupt(exception_type)

  def on_finishing(self, result: _Result[T]) -> None:
    """Handles the completion of the `task()` function call.

    Called by the worker thread after `interruption_scope.run()` has exited
    and cleared any pending async-injected exception.

    State transitions:
      RUNNING -> DONE:
        Normal completion; stores the worker's result and fires done_callbacks.
      INITIAL | CANCELLING -> DONE:
        The worker finished after cancellation was requested; marks cancelled
        and fires done_callbacks.
      DONE -> DONE (no-op):
        Happens when `on_running()` returned False after `INITIAL -> DONE`.

    Args:
      result: The result of the task function (value or caught exception).
    """
    done_callbacks = []
    with self.lock:
      assert self.state in (
          self.STATE_INITIAL,
          self.STATE_RUNNING,
          self.STATE_CANCELLING,
          self.STATE_DONE,
      )
      if self.state is not self.STATE_DONE:
        if (
            self.state is self.STATE_RUNNING
            and not self.cancellation_context.cancelled()
        ):
          self.result = result
        self.state = self.STATE_DONE
        self.result_ready.set()
        done_callbacks, self.done_callbacks = self.done_callbacks, []
    self.cancellation_subscription.unsubscribe()
    self._run_callbacks(done_callbacks)

  def add_done_callback(self, cb: Callable[[], Any]) -> None:
    """Registers `cb` to be invoked when the computation is done.

    If the computation is already done, `cb` is called immediately.

    Args:
      cb: The callback to be invoked when the computation is done.
    """
    done_callbacks = []
    with self.lock:
      if self.state is self.STATE_DONE:
        done_callbacks.append(cb)
      else:
        self.done_callbacks.append(cb)
    self._run_callbacks(done_callbacks)

  @classmethod
  def _run_callbacks(cls, done_callbacks: list[Callable[[], Any]]) -> None:
    """Runs the done_callbacks outside of the lock to avoid deadlock."""
    # This method must be called outside of `self.lock` to avoid deadlock.
    for cb in done_callbacks:
      try:
        cb()
      except BaseException:  # pylint: disable=broad-exception-caught
        pass


class TaskFuture[T]:
  """Handle for a cancellable asynchronous task result.

  NOTE: Similar to `concurrent.futures.Future`, but supports cooperative
  cancellation based on `arolla.abc.CancellationContext` and forced interruption
  via `interrupt()`.
  """

  __slots__ = ('_state',)

  _state: _State[T]

  def __init__(self, *, state: _State[T]) -> None:
    """(internal) Constructs a TaskFuture."""
    self._state = state

  def __repr__(self) -> str:
    prefix = f'<TaskFuture at 0x{id(self):x}'
    if not self._state.result_ready.is_set():
      if self._state.is_cancelled():
        return f'{prefix} state=cancelling>'
      return f'{prefix} state=pending>'
    result = self._state.result
    if result is None:
      return f'{prefix} state=cancelled>'
    assert result.is_exception() != result.is_value()
    if result.is_exception():
      return (
          f'{prefix} state=finished raised'
          f' {arolla.abc.get_type_name(type(result.exception))}>'
      )
    return (
        f'{prefix} state=finished returned'
        f' {arolla.abc.get_type_name(type(result.value))}>'
    )

  @property
  def cancellation_context(self) -> arolla.abc.CancellationContext:
    """Returns the `CancellationContext` associated with this task."""
    return self._state.cancellation_context

  def done(self) -> bool:
    """Returns True if the computation has finished.

    Also returns True if the task was cancelled before it started running.
    """
    return self._state.result_ready.is_set()

  def cancelled(self) -> bool:
    """Returns True if cancellation of the task was requested."""
    return self._state.is_cancelled()

  def cancel(self) -> _NonBool:
    """Requests cooperative cancellation of the future.

    If the task has not started running yet, cancels it in the executor and
    immediately transitions the future to the done state with `CancelledError`.
    If the task is currently running, signals its `cancellation_context`; the
    future transitions to the done state with `CancelledError` once the worker
    finishes executing the task function.

    Returns:
      A `_NonBool` object that prevents accidental truth-value checks,
      avoiding ambiguity with `concurrent.futures.Future.cancel()`.
    """
    self._state.on_cancel()
    return _NON_BOOL

  def interrupt(
      self, exception_type: type[BaseException] = KeyboardInterrupt
  ) -> None:
    """Interrupts the task by injecting an async exception into the worker.

    Signals cooperative cancellation (as in `cancel()`) and, if the task is
    currently running, asynchronously injects `exception_type` into the worker
    thread. Can be called multiple times. The future transitions to the done
    state with `CancelledError` once the worker finishes executing the task
    function.

    WARNING: Asynchronous exception injection is a last-resort mechanism that
    can raise at arbitrary Python bytecode boundaries, including inside
    cancellation cleanup or `finally` blocks. Callers should normally request
    cooperative cancellation via `cancel()` first and wait for a grace period
    (e.g. via `wait(timeout=...)` or `schedule_callback_after(...)`) before
    calling `interrupt()`.

    Args:
      exception_type: The `BaseException` subclass to inject into the worker
        thread (defaults to `KeyboardInterrupt`).
    """
    self._state.on_interrupt(exception_type)

  def wait(self, timeout: float | None = None) -> bool:
    """Waits for the computation to finish.

    Args:
      timeout: The maximum time in seconds to wait. If None, wait indefinitely.

    Returns:
      True if the computation is done; False if the timeout expired.
    """
    return self._state.result_ready.wait(timeout=timeout)

  def result(self, timeout: float | None = None) -> T:
    """Returns the result stored in the future.

    If the future is not yet done, this method will block until the computation
    finishes, or until the timeout expires. If the future was cancelled, raises
    `futures.CancelledError`. If the task raised an exception, it will be
    re-raised. If timeout expires, a `futures.TimeoutError` is raised.

    Args:
      timeout: The maximum time in seconds to wait for the result. If None, wait
        indefinitely.

    Raises:
      futures.TimeoutError: If the timeout expires before the task finishes.
      futures.CancelledError: If the task was cancelled.
    """
    if not self._state.result_ready.wait(timeout=timeout):
      raise futures.TimeoutError()
    result = self._state.result
    if result is None:
      raise futures.CancelledError()
    assert result.is_exception() != result.is_value()
    if result.is_exception():
      raise result.exception
    return result.value

  def add_done_callback(self, fn: Callable[[Self], Any]) -> None:
    """Registers a callback to be invoked when the computation finishes.

    If the future is already done, `fn` is called immediately (synchronously).

    NOTE: Avoid doing expensive operations in the callback; if you need to run
    an expensive/blocking operation, consider scheduling a task on a different
    thread.

    Args:
      fn: The callback to be invoked when the future is done.
    """
    self._state.add_done_callback(lambda: fn(self))


def submit[**P, R](
    executor: futures.ThreadPoolExecutor,
    fn: Callable[P, R],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> TaskFuture[R]:
  """Submits a callable to a thread pool executor and returns a TaskFuture.

  The task runs inside an `arolla.abc.CancellationContext` and an
  `arolla.experimental.tasks.InterruptionScope`, enabling cooperative
  cancellation via `TaskFuture.cancel()` (or
  `TaskFuture.cancellation_context.cancel()`) and forced interruption via
  `TaskFuture.interrupt()`.

  Args:
    executor: The thread pool executor to submit the task to.
    fn: The callable to execute.
    *args: Positional arguments forwarded to `fn`.
    **kwargs: Keyword arguments forwarded to `fn`.

  Returns:
    A `TaskFuture` that tracks the result and supports cancellation.
  """
  state = _State[R]()

  def run_task(result: _Result[R]) -> None:
    if not state.cancellation_context.cancelled():
      result.value = fn(*args, **kwargs)

  def worker() -> None:
    result = _Result[R]()
    try:
      if state.on_running():
        state.interruption_scope.run(run_task, result)
    except BaseException as ex:  # pylint: disable=broad-exception-caught
      result.exception = ex
    finally:
      state.on_finishing(result)

  with state.lock:
    internal_future = executor.submit(
        arolla.abc.run_in_cancellation_context,
        state.cancellation_context,
        worker,
    )
    state.internal_future = internal_future
    # NOTE: Subscribe outside `state.lock` to avoid a potential deadlock
    # if `internal_future` was already cancelled (e.g., by executor shutdown).
  internal_future.add_done_callback(state.on_internal_future_done)
  return TaskFuture(state=state)
