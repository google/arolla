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

"""(Internal) Callback bridge for scheduling python callbacks on C++ events.

Please use the public API: `from arolla.experimental import tasks`.
"""

import atexit
import threading
from typing import Any, Callable
from arolla import arolla
from arolla.experimental._tasks import clib

CancellationContextSubscription = clib.CancellationContextSubscription

_callback_bridge = None
_callback_bridge_init_lock: threading.Lock = threading.Lock()


def default_callback_bridge() -> clib.PythonCallbackBridge:
  """Returns the default callback bridge."""
  global _callback_bridge
  if _callback_bridge is None:
    with _callback_bridge_init_lock:
      if _callback_bridge is None:
        _callback_bridge = clib.PythonCallbackBridge()
        # Register the close method with atexit to stop the worker thread before
        # the python interpreter finalizes.
        atexit.register(_callback_bridge.close)
  return _callback_bridge


def subscribe_to_cancellation(
    callback: Callable[[], Any],
    *,
    cancellation_context: arolla.abc.CancellationContext | None = None,
) -> CancellationContextSubscription:
  """Subscribes a callback to the given cancellation context.

  Args:
    callback: The callback to invoke when the cancellation context is cancelled.
      Avoid heavy computation inside the callback, as it can delay the delivery
      of other cancellation events. If non-trivial work is required, consider
      scheduling it on a separate thread.
    cancellation_context: The cancellation context to subscribe to. If not
      specific, the current cancellation context will be used.

  Returns:
    A subscription object that acts as a context manager, if you need to
    unsubscribe the callback after the context is exited.
  """
  return clib.subscribe_to_cancellation(
      default_callback_bridge(), callback, cancellation_context
  )
