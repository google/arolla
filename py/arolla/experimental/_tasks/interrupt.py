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

"""(Internal) Utilities for thread interruption."""

from collections.abc import Callable, Iterator
import contextlib
from typing import Any

from arolla.experimental._tasks import clib

InterruptionScope = clib.InterruptionScope


def _raise_pending_async_exc() -> None:
  """Executes python code so any pending async exception is raised."""


@contextlib.contextmanager
def _subscribe_cm(callback: Callable[[], Any]) -> Iterator[None]:
  """Context-manager implementation for `subscribe_to_interrupt`."""
  subscription = clib.InterruptionSubscription()
  try:
    subscription.subscribe(callback)
    _raise_pending_async_exc()
    yield
  finally:
    subscription.unsubscribe()


def subscribe_to_interrupt(
    callback: Callable[[], Any], /
) -> contextlib.AbstractContextManager[None]:
  """Returns a context manager that subscribes `callback` to `interrupt()`.

  While inside the `with` block on a thread running `InterruptionScope.run()`,
  calling `interrupt()` on that scope invokes `callback()` synchronously on the
  interrupting thread instead of injecting an async exception into the worker
  thread. When multiple subscriptions are active, callbacks are invoked from
  the most recent subscription to the oldest one; if a callback raises an
  exception, remaining callbacks are skipped and the exception is propagated to
  the caller.

  Entering the context manager outside an active `InterruptionScope` is a no-op.

  Args:
    callback: Zero-argument callable invoked synchronously on `interrupt()`.
  """
  if not callable(callback):
    raise TypeError(
        'arolla_tasks.subscribe_to_interrupt() expected a callable, got'
        f' callback: {type(callback).__name__}'
    )
  return _subscribe_cm(callback)
