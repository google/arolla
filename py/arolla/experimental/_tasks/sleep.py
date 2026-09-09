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

"""(Internal) Cancellation-aware sleep.

Please use the public API: `from arolla.experimental import tasks`.
"""

import os
import threading

from arolla import arolla
from arolla.experimental._tasks import callbacks


def sleep(duration: float) -> None:
  """Sleeps for `duration` seconds, waking early if cancelled.

  Args:
    duration: The time to sleep, in seconds. Must be non-negative.

  Raises:
    ValueError: If `duration` is negative, or if the cancellation context
      has been cancelled.
  """
  if duration < 0:
    raise ValueError(f'sleep duration must be non-negative, got {duration}')
  arolla.abc.raise_if_cancelled()
  if duration == 0:
    os.sched_yield()
    return
  wakeup = threading.Event()
  if not arolla.abc.current_cancellation_context():
    wakeup.wait(timeout=duration)
    return
  with callbacks.subscribe_to_cancellation(
      wakeup.set
  ):
    wakeup.wait(timeout=duration)
  arolla.abc.raise_if_cancelled()
