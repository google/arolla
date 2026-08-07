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

import asyncio
from concurrent import futures
import ctypes
import threading
from typing import Any, Callable, Self

from arolla import arolla

_event_loop = asyncio.new_event_loop()
_event_loop_thread = threading.Thread(
    target=_event_loop.run_forever,
    daemon=True,
    name='arolla.experimental._tasks.CancellationEventLoop',
)  # started by _ensure_event_loop_thread_started()
_event_loop_thread_lock = threading.Lock()


def _ensure_event_loop_thread_started():
  """Lazily starts the background event loop thread."""
  if _event_loop_thread.ident is not None:
    return
  with _event_loop_thread_lock:
    if _event_loop_thread.ident is None:
      _event_loop_thread.start()


class _NonBool:
  """An utility class that always raises an error on bool(self)."""

  __slots__ = ()

  def __bool__(self):
    raise TypeError('Truth value of this object is ambiguous.')


_NON_BOOL = _NonBool()


class _Result[T]:
  """An utility structure that stores either an exception or a value."""

  __slots__ = ('exception', 'value')

  exception: BaseException
  value: T

  def is_exception(self) -> bool:
    return hasattr(self, 'exception')

  def is_value(self) -> bool:
    return hasattr(self, 'value')


_CANCELLED_RESULT = _Result[Any]()
_CANCELLED_RESULT.exception = futures.CancelledError()


class _State[T]:
  """The internal state of a TaskFuture.

  A thread-safe state machine managing the lifecycle of a task submitted to
  a thread pool executor. All state transitions are guarded by `self.lock`.
  The machine supports cooperative cancellation with escalation to forced
  (async-injected) interruption.

  States:
    INITIAL: The task has been submitted but the worker thread has not yet
      started executing the task function.
    RUNNING: The worker thread is actively executing the task function.
    DONE: Terminal state. The task has completed (successfully, with an
      exception, or via cancellation).
    CANCELLING: Cancellation was requested while the task was RUNNING. The
      cancellation context has been signaled, and a timer has been scheduled
      to escalate to FORCE_CANCELLING after FORCE_CANCELLATION_DELAY_SECONDS.
      From the consumer's perspective, the result is already available
      (CancelledError), but the worker thread may still be running.
    FORCE_CANCELLING: Terminal state. The grace period expired and the worker
      thread did not stop; a KeyboardInterrupt has been async-injected.
      Because the injected exception can strike at any point,
      a transition to DONE cannot be made reliably.

  Result and callback semantics:
    - `result_ready` is set as soon as a result is available to the consumer.
      In the CANCELLING path, this happens before the worker thread has
      actually stopped, so consumers unblock immediately with CancelledError.
    - Callbacks are fired exactly once, upon the first transition that sets
      `result_ready`. They are always executed outside `self.lock` to prevent
      deadlocks.

  Main state transition paths:
    INITIAL -> RUNNING -> DONE:
      The computation starts and completes.
    INITIAL -> DONE:
      The computation is cancelled before the worker thread started execution.
    INITIAL -> RUNNING -> CANCELLING -> DONE:
      The computation was cancelled, but stopped before the grace period
      expired.
    INITIAL -> RUNNING -> CANCELLING -> FORCE_CANCELLING:
      The computation was cancelled, but did not stop before the grace period
      expired.
  """

  FORCE_CANCELLATION_DELAY_SECONDS = 30.0

  STATE_INITIAL = 'INITIAL'
  STATE_RUNNING = 'RUNNING'
  STATE_DONE = 'DONE'  # terminal state
  STATE_CANCELLING = 'CANCELLING'
  STATE_FORCE_CANCELLING = 'FORCE_CANCELLING'  # terminal state

  __slots__ = (
      # go/keep-sorted start
      'callbacks',
      'cancellation_context',
      'internal_future',
      'lock',
      'result',
      'result_ready',
      'state',
      'thread',
      # go/keep-sorted end
  )

  # go/keep-sorted start
  callbacks: list[Callable[[], Any]]
  cancellation_context: arolla.abc.CancellationContext
  internal_future: futures.Future[None]
  lock: threading.Lock
  result: _Result[T]
  result_ready: threading.Event
  state: str
  thread: threading.Thread
  # go/keep-sorted end

  def __init__(self) -> None:
    # go/keep-sorted start
    self.callbacks = []
    self.cancellation_context = arolla.abc.CancellationContext()
    self.lock = threading.Lock()
    self.result_ready = threading.Event()
    self.state = self.STATE_INITIAL
    # go/keep-sorted end

  def on_running(self) -> bool:
    """Handles the start of `task()`.

    Called by the worker thread.

    State transitions:
      INITIAL -> RUNNING:
        Records the current thread for potential forced cancellation.
      DONE -> DONE (no-op):
        Cancellation occurred before `task()` could report it was actually
        running.

    Returns:
      True if the state transitioned to RUNNING; otherwise, False.
    """
    with self.lock:
      assert self.state in (self.STATE_INITIAL, self.STATE_DONE)
      if self.state is self.STATE_INITIAL:
        self.state = self.STATE_RUNNING
        self.thread = threading.current_thread()
        return True
      return False

  def on_internal_future_done(self, f: futures.Future[None]) -> None:
    """Handles cancellation of the internal `concurrent.futures.Future`.

    Invoked when the internal future completes. Only acts when the future
    was cancelled (e.g. the executor shut down, or `on_cancel()` cancelled
    the future before the worker started).

    State transitions:
      INITIAL -> DONE:
        `task()` hasn't started yet.
      DONE -> DONE (no-op):
        Another cancellation path (e.g. a parallel `on_cancel()` call) already
        transitioned to DONE.

    Args:
      f: The internal future.
    """
    if not f.cancelled():
      return
    callbacks = []
    try:
      with self.lock:
        assert self.state in (self.STATE_INITIAL, self.STATE_DONE)
        if self.state is self.STATE_INITIAL:
          self.state = self.STATE_DONE
          self.result = _CANCELLED_RESULT
          self.result_ready.set()
          callbacks = self.callbacks
          self.callbacks = []
    finally:
      self._run_callbacks(callbacks)

  def on_cancel(self) -> None:
    """Handles an explicit cancellation request from the consumer.

    State transitions:
      INITIAL -> DONE:
        `task()` hasn't started yet.
      RUNNING -> CANCELLING:
        `task()` started but hasn't finished yet. Sends a signal to the
        cancellation context and schedules a forced cancellation after a grace
        period.
      All other states remain unchanged: the task is either already done or
        in the process of cancellation.
    """
    # NOTE: Immediately try to cancel the underlying future.
    self.internal_future.cancel()
    callbacks = []
    try:
      with self.lock:
        if self.state is self.STATE_INITIAL:
          self.state = self.STATE_DONE
          self.result = _CANCELLED_RESULT
          self.result_ready.set()
          callbacks = self.callbacks
          self.callbacks = []
        elif self.state is self.STATE_RUNNING:
          self.state = self.STATE_CANCELLING
          self.result = _CANCELLED_RESULT
          self.result_ready.set()
          callbacks = self.callbacks
          self.callbacks = []
          _event_loop.call_soon_threadsafe(
              _event_loop.call_later,
              self.FORCE_CANCELLATION_DELAY_SECONDS,
              self.on_force_cancel,
          )
    finally:
      self.cancellation_context.cancel('TaskFuture.cancel() was called')
      self._run_callbacks(callbacks)

  def on_force_cancel(self) -> None:
    """Handles the end of the graceful cancellation period.

    Called by the background event loop timer after
    FORCE_CANCELLATION_DELAY_SECONDS has elapsed since entering CANCELLING.

    State transitions:
      CANCELLING -> FORCE_CANCELLING:
        Injects a KeyboardInterrupt via PyThreadState_SetAsyncExc.
      DONE -> DONE (no-op):
        The worker already finished during the grace period.
    """
    with self.lock:
      assert self.state in (self.STATE_CANCELLING, self.STATE_DONE)
      if self.state is self.STATE_CANCELLING:
        self.state = self.STATE_FORCE_CANCELLING
        thread_id = self.thread.ident
        if thread_id is not None and self.thread.is_alive():
          ctypes.pythonapi.PyThreadState_SetAsyncExc(
              ctypes.c_ulong(thread_id), ctypes.py_object(KeyboardInterrupt)
          )

  def on_finishing(self, result: _Result[T]) -> None:
    """Handles the completion of the `task()` function call.

    Called by the worker thread.

    Postcondition: After this method finishes, there should be no pending
    async-injected KeyboardInterrupt in the worker thread.

    State transitions:
      RUNNING -> DONE:
        Normal completion; stores the worker's result and fires callbacks.
      CANCELLING -> DONE:
        The worker finished after cancellation was requested but before the
        grace period expired. The result was already set by `on_cancel()`.
      FORCE_CANCELLING -> FORCE_CANCELLING (no-op):
        The worker finished after the grace period expired. Best-effort
        cleanup of a possibly pending async-injected exception.
      DONE -> DONE (no-op):
        Happens when `on_running()` was transitioning DONE -> DONE.

    Args:
      result: The result of the task function (value or caught exception).
    """
    callbacks = []
    try:
      with self.lock:
        assert self.state in (
            self.STATE_RUNNING,
            self.STATE_CANCELLING,
            self.STATE_FORCE_CANCELLING,
            self.STATE_DONE,
        )
        if self.state is self.STATE_RUNNING:
          self.state = self.STATE_DONE
          self.result = result
          self.result_ready.set()
          callbacks = self.callbacks
          self.callbacks = []
        elif self.state is self.STATE_CANCELLING:
          self.state = self.STATE_DONE
          assert self.result_ready.is_set()
        elif self.state is self.STATE_FORCE_CANCELLING:
          # NOTE: Since the async-injected KeyboardInterrupt could strike at any
          # moment (including the previous line where we check `self.state`),
          # we cannot reliably transition to the DONE state. Thus, we leave it
          # as FORCE_CANCELLING, treating it as another terminal state.

          # Best effort to clean up a possibly pending async-injected exception
          # to prevent it from being fired in the future.
          ctypes.pythonapi.PyThreadState_SetAsyncExc(
              ctypes.c_ulong(self.thread.ident), None  # pyrefly: ignore[bad-argument-type]
          )
          # A sanity check: we don't need to check if `self.thread` is alive,
          # because it's the current thread.
          assert self.thread is threading.current_thread()
        else:  # self.state is self.STATE_DONE
          pass
    finally:
      self._run_callbacks(callbacks)

  def add_callback(self, cb: Callable[[], Any]) -> None:
    """Registers `cb` to be invoked when the result is available.

    If the result is already set, `cb` is called immediately.

    Args:
      cb: The callback to be invoked when the result is available.
    """
    callbacks = []
    try:
      with self.lock:
        if self.result_ready.is_set():
          callbacks.append(cb)
        else:
          self.callbacks.append(cb)
    finally:
      self._run_callbacks(callbacks)

  @classmethod
  def _run_callbacks(cls, callbacks: list[Callable[[], Any]]) -> None:
    """Runs the callbacks outside of the lock to avoid deadlock."""
    # This method must be called outside of `self.lock` to avoid deadlock.
    for cb in callbacks:
      try:
        cb()
      except BaseException:  # pylint: disable=broad-exception-caught
        pass


class TaskFuture[T]:
  """Handle for a cancellable asynchronous task result.

  NOTE: Similar to `concurrent.futures.Future`, but supports cooperative
  cancellation based on `arolla.abc.CancellationContext`.
  """

  __slots__ = ('_state',)

  _state: _State[T]

  def __init__(self, *, state: _State[T]) -> None:
    """(internal) Constructs a TaskFuture."""
    self._state = state

  def __repr__(self) -> str:
    prefix = f'<TaskFuture at 0x{id(self):x}'
    if not self._state.result_ready.is_set():
      return f'{prefix} state=pending>'
    result = self._state.result
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

  def done(self) -> bool:
    """Returns True if the computation is finished or cancelled."""
    return self._state.result_ready.is_set()

  def cancel(self) -> _NonBool:
    """Requests cancellation of the future.

    If the future is not yet done, sets the CancelledError exception as
    the result and cancels the internal cancellation context.

    Returns:
      A `_NonBool` object that prevents accidental truth-value checks,
      avoiding ambiguity with `concurrent.futures.Future.cancel()`.
    """
    self._state.on_cancel()
    return _NON_BOOL

  def result(self, timeout: float | None = None) -> T:
    """Returns the result stored in the future.

    If the future is not yet done, this method will block until it is done, or
    until the timeout expires. If the future stores an exception, it will be
    raised. If timeout expires, a `TimeoutError` is raised.

    Args:
      timeout: The maximum time in seconds to wait for the result. If None, wait
        indefinitely.

    Raises:
      futures.TimeoutError: If the timeout expires.
    """
    if not self._state.result_ready.wait(timeout=timeout):
      raise futures.TimeoutError()
    result = self._state.result
    assert result.is_exception() != result.is_value()
    if result.is_exception():
      raise result.exception
    return result.value

  def add_done_callback(self, fn: Callable[[Self], Any]) -> None:
    """Registers a callback to be invoked when the operation completes.

    If the future is already done, `fn` is called immediately (synchronously).

    NOTE: Avoid doing expensive operations in the callback; if you need to run
    an expensive/blocking operation, consider scheduling a task on a different
    thread.

    Args:
      fn: The callback to be invoked when the future is done.
    """
    self._state.add_callback(lambda: fn(self))


def submit[**P, R](
    executor: futures.ThreadPoolExecutor,
    fn: Callable[P, R],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> TaskFuture[R]:
  """Submits a callable to a thread pool executor and returns a TaskFuture.

  The task runs inside an `arolla.abc.CancellationContext`, enabling
  cooperative cancellation. If the consumer calls `TaskFuture.cancel()`,
  the cancellation context is signaled and, after a grace period,
  a `KeyboardInterrupt` is async-injected into the worker thread.

  Args:
    executor: The thread pool executor to submit the task to.
    fn: The callable to execute.
    *args: Positional arguments forwarded to `fn`.
    **kwargs: Keyword arguments forwarded to `fn`.

  Returns:
    A `TaskFuture` that tracks the result and supports cancellation.
  """
  _ensure_event_loop_thread_started()
  state = _State[R]()

  def worker() -> None:
    nonlocal state, fn, args, kwargs
    result = _Result[R]()
    try:
      if state.on_running():
        result.value = fn(*args, **kwargs)
    except BaseException as ex:  # pylint: disable=broad-exception-caught
      result.exception = ex
    finally:
      # NOTE: If the thread was force-cancelled, this `finally` can be
      # interrupted in the middle. However, this is not an issue, since in
      # such an event, the resulting future will already be in a valid terminal
      # state.
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
