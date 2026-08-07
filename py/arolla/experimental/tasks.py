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

from arolla.experimental._tasks import callbacks as _callbacks
from arolla.experimental._tasks import submit as _submit

# go/keep-sorted start
CancellationContextSubscription = _callbacks.CancellationContextSubscription
TaskFuture = _submit.TaskFuture
submit = _submit.submit
subscribe_to_cancellation = _callbacks.subscribe_to_cancellation
# go/keep-sorted end
