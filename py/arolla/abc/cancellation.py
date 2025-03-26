# Copyright 2024 Google LLC
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

"""Cancellation facilities."""

from arolla.abc import clib

# A cancellation context class.
#
# Cancellation context is a primitive for signaling cancellation across
# multiple control flows.
#
# To make a cancellation context 'current' within the current control flow,
# use `arolla.abc.run_in_cancellation_context()`. To work with the current
# cancellation context, use:
#
#   * arolla.abc.current_cancellation_context()
#   * arolla.abc.cancelled()
#   * arolla.abc.raise_if_cancelled()
#
# It's safe to share a cancellation context object between threads.
#
# class CancellationContext:
#
#   Methods defined here:
#
#     __new__(cls)
#       Constructs an instance of the class.
#
#     cancel(msg='', /)
#       Cancels the context.
#
#     cancelled()
#       Returns `True` if the context is cancelled.
#
#     raise_if_cancelled()
#       Raises an exception if the context is cancelled.
#
# (implementation: py/arolla/abc/py_cancellation.cc)
#
CancellationContext = clib.CancellationContext

# run_in_cancellation_context(cancellation_context, fn, /, *args, **kwargs)
# ---
#
# Runs `fn(*args, **kwargs)` in the given cancellation context.
run_in_cancellation_context = clib.run_in_cancellation_context

# run_in_default_cancellation_context(fn, /, *args, **kwargs)
# ---
#
# Runs `fn(*args, **kwargs)` in the default cancellation context.
#
# The default cancellation context is determined as follows:
#   1) Keep the current cancellation context, if available.
#   2) Otherwise, if running on Python's main thread, use a context that
#      reacts to SIGINT.
#   3) Otherwise, create a new cancellation context.
run_in_default_cancellation_context = clib.run_in_default_cancellation_context

# Returns the current cancellation context or None.
current_cancellation_context = clib.current_cancellation_context

# Returns `True` if the current cancellation context is cancelled.
#
# Note: If you use this function within a performance-critical loop,
# consider storing it in a local variable:
#
#   cancelled = arolla.abc.cancelled
#   while ... and not cancelled():
#     ...
cancelled = clib.cancelled

# Raises an exception if the current cancellation context is cancelled.
#
# Note: If you use this function within a performance-critical loop,
# consider storing it in a local variable:
#
#   raise_if_cancelled = arolla.abc.raise_if_cancelled
#   while ...:
#     raise_if_cancelled()
raise_if_cancelled = clib.raise_if_cancelled

# Simulate the effect of SIGINT on the existing cancellation contexts.
simulate_SIGINT = clib.simulate_SIGINT
