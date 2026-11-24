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

"""Experimental API for working with cancellable tasks in Python."""

from arolla.experimental._tasks import event_loop as _event_loop
from arolla.experimental._tasks import interrupt as _interrupt
from arolla.experimental._tasks import locks as _locks
from arolla.experimental._tasks import sleep as _sleep
from arolla.experimental._tasks import submit as _submit

# go/keep-sorted start
InterruptionScope = _interrupt.InterruptionScope
Lock = _locks.Lock
TaskFuture = _submit.TaskFuture
schedule_callback_after = _event_loop.schedule_callback_after
sleep = _sleep.sleep
submit = _submit.submit
# go/keep-sorted end
