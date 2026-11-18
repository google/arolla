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

"""(Internal) Provides a default event loop.

Please use the public API:

   from arolla.experimental import tasks as arolla_tasks
"""

import asyncio
import atexit
import contextvars
import threading
from typing import Any, Callable
import warnings

from arolla import arolla

_event_loop = asyncio.new_event_loop()
_event_loop_thread = threading.Thread(
    target=_event_loop.run_forever,
    daemon=True,
    name='arolla.experimental._tasks.EventLoop',
)
_event_loop_lock = threading.Lock()


def _shutdown_event_loop() -> None:
  if _event_loop_thread.ident is not None:
    _event_loop.call_soon_threadsafe(
        _event_loop.stop, context=contextvars.Context()
    )
    _event_loop_thread.join()


def _ensure_event_loop_thread_started() -> None:
  if _event_loop_thread.ident is not None:
    return
  with _event_loop_lock:
    if _event_loop_thread.ident is None:
      _event_loop_thread.start()
      atexit.register(_shutdown_event_loop)


def schedule_callback_after[**P](
    seconds: float,
    callback: Callable[P, Any],
    /,
    *args: P.args,
    **kwargs: P.kwargs,
) -> None:
  """Schedules `callback(*args, **kwargs)` to run after `seconds` have elapsed.

  NOTE: `callback` runs on a shared background thread and MUST be fast and
  non-blocking; slow callbacks will delay other scheduled callbacks.

  Args:
    seconds: Non-negative delay in seconds.
    callback: Fast, non-blocking callable to invoke after `seconds`.
    *args: Positional arguments forwarded to `callback`.
    **kwargs: Keyword arguments forwarded to `callback`.
  """
  if not seconds >= 0:  # Handles NaN
    raise ValueError(f'`seconds` must be non-negative, got {seconds}')
  if not callable(callback):
    raise TypeError(
        '`callback` must be callable, got'
        f' {arolla.abc.get_type_name(type(callback))}'
    )

  def _run_callback() -> None:
    try:
      callback(*args, **kwargs)
    except BaseException as ex:  # pylint: disable=broad-exception-caught
      warnings.warn(
          'arolla_tasks.schedule_callback_after: '
          f'unhandled exception in callback {callback!r}: {ex!r}',
          RuntimeWarning,
      )

  _ensure_event_loop_thread_started()
  _event_loop.call_soon_threadsafe(
      _event_loop.call_at,
      _event_loop.time() + seconds,
      _run_callback,
      context=contextvars.Context(),
  )
