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

"""Facilities for Cooperative Cancellation.

## TL;DR
  * arolla.abc.raise_if_cancelled():
    Raises an exception if the current cancellation context is cancelled.

  * arolla.abc.current_cancellation_context():
    Returns the current cancellation context, or `None` if not set.

  * arolla.abc.run_in_cancellation_context(
        cancellation_context, fn, *args, **kwargs
    ):
    Runs `fn(*args, **kwargs)` with the provided `cancellation_context` set as
    current.

  * @arolla.abc.add_default_cancellation_context
    Decorator that provides a default cancellation context for a function call
    if no current context is already set.


## Overview with more details

Arolla provides mechanisms for cooperative cancellation, allowing long-running
operations to be interrupted gracefully. The fundamental building block is
the `arolla.abc.CancellationContext` class, which works as a synchronization
primitive for signaling cancellation across multiple control flows.

For most scenarios, the recommended approach is to only interact with
the "current" cancellation context associated with the current thread. You can
check its status using the functions `arolla.abc.raise_if_cancelled()` (which
raises an exception if the current context is cancelled) and
`arolla.abc.cancelled()` (which returns a boolean result). The C++ equivalents
are `arolla::CheckCancellation()` and `arolla::Cancelled()`, respectively.

When scheduling a subtask on another thread or in a thread pool, the recommended
approach is to obtain the current context using
`arolla.abc.current_cancellation_context()`, pass this context object along
with the task parameters, and use `arolla.abc.run_in_cancellation_context()` on
the worker thread to run the subtask within that context.
In C++, the current cancellation context can be obtained using
`arolla::CancellationContext::ScopeGuard::current_cancellation_context()`;
managing is done using the `arolla::CancellationContext::ScopeGuard` RAII class.

The decorator `@arolla.abc.add_default_cancellation_context` is designed for
user-facing functions to automatically provide a cancellation context.
When the decorated function is called, if a context already exists,
the decorator takes no action, allowing the current cancellation context to
remain in use. Otherwise, it provides a default cancellation context.
If the call happens on Python's main thread, the provided cancellation context
will be sensitive to SIGINT, similar to the KeyboardInterrupt mechanism.


## Example: Parallel Map with Cancellation

This example demonstrates using cancellation with a thread pool. Hitting Ctrl+C
while `gen_squares` is running will trigger cancellation.


    import concurrent.futures
    import functools
    import time

    from arolla import arolla


    @arolla.abc.add_default_cancellation_context  # <- decorator will provide
                                                  # a default cancellation
                                                  # context during the call,
                                                  # if none is set
    def parallel_map(fn, *args, **kwargs):
      executor = concurrent.futures.ThreadPoolExecutor()
      try:
        cancellation_context = arolla.abc.current_cancellation_context()
        return list(
            executor.map(
              functools.partial(
                arolla.abc.run_in_cancellation_context,  # <- setup cancellation
                cancellation_context,                    # context on the worker
                fn,                                      # thread
              ),
              *args,
              **kwargs,
            )
        )
      finally:
        executor.shutdown()


    def cancellable_sleep(seconds):
      stop = time.time() + seconds
      while time.time() < stop:
        arolla.abc.raise_if_cancelled()  # <- the cancellation mechanism is
                                         # cooperative, so we need to do
                                         # periodic checks
        time.sleep(0.1)


    def gen_squares(n, *, sleep_seconds=0.0):
      def fn(x):
        cancellable_sleep(sleep_seconds)  # <- emulate a long computation
        return x * x

      return parallel_map(fn, range(n))


    gen_squares(10, sleep_seconds=100)
    # Hitting CTRC+C cancels the "default" context.
"""

import functools
import signal
import sys
import threading
import types
from typing import Callable, ParamSpec, TypeVar
import warnings

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
# Runs `fn(*args, **kwargs)` in a cancellation context.
#
# The cancellation context is determined as follows:
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

# Overrides the signal handler for SIGINT.
#
# This function is unsafe because it replaces the existing SIGINT\
# handler, potentially bypassing other signal handlers and directly
# forwarding the signal to the Python interpreter. However, it might
# be considered safe if the previous handler was set by Python.
unsafe_override_signal_handler_for_cancellation = (
    clib.unsafe_override_signal_handler_for_cancellation
)

P = ParamSpec('P')
R = TypeVar('R')


# Fix function.partial to correctly handles binding for instance methods.
#
# TODO: Remove this class when `functools.partial` becomes
# a method descriptor (https://github.com/python/cpython/pull/121092).
class _Partial(functools.partial):
  __slots__ = ()

  def __get__(self, obj, objtype):
    if obj is None:
      return self
    return types.MethodType(self, obj)


def add_default_cancellation_context(fn: Callable[P, R]) -> Callable[P, R]:
  """Decorator to ensure the callable runs within a cancellation context.

  The cancellation context is determined as follows:
    1) Keep the current cancellation context, if available.
    2) Otherwise, if running on Python's main thread, use a context that
       reacts to SIGINT.
    3) Otherwise, create a new cancellation context.

  Args:
    fn: A callable.

  Returns:
    A wrapped callable.
  """
  return functools.update_wrapper(
      _Partial(run_in_default_cancellation_context, fn), fn
  )


################################################################################
# Unfortunately, Python provides limited API for detecting interactive         #
# interrupt events, that various libraries and frameworks have to compete for. #
#                                                                              #
# By default, Arolla sets up a native signal handler for SIGINT, with          #
# forwarding signals to the previous signal handler. This is a minimal and     #
# safe configuration. However, it's not sufficient in scenarios when other     #
# libraries and frameworks override the signal handler without forwarding.     #
#                                                                              #
# Below we have custom integrations with known systems, like Jupyter or        #
# Google Colab notebooks, to provide a better user experience.                 #
################################################################################


# When the IPython shell is kernel-based, we cannot rely on a signal handler
# for cancellation purposes, because `kernel.pre_handler_hook` routinely resets
# it before running any cell.
#
# The (hopefully) temporary solution is to reinstall the signal handler on
# the 'pre_run_cell' event.


def _ipython_events_pre_run_cell_hook():
  """A custom `pre_run_cell` hook that reinstalls SIGINT handler."""
  if threading.current_thread() is not threading.main_thread():
    # We assume that `pre_run_cell` hook runs on the same thread as the cell.
    # If IPython is set up to run cells off the main thread, KeyboardInterrupt
    # is not expected to work within the cells, including for Arolla,
    # so no action is needed.
    # (Also, technically, the signal handlers are supposed to be installed only
    # from the main thread.)
    return
  previous_int_handler = signal.getsignal(signal.SIGINT)
  if previous_int_handler in (signal.SIG_IGN, signal.SIG_DFL):
    # `SIG_IGN` indicates that the signal is intended to be ignored; thus, we
    # keep this behaviour.
    #
    # `SIG_DFL` indicates the default handler, which terminates the process
    # for SIGINT; thus our custom handing would be redundant.
    return
  if previous_int_handler is not signal.default_int_handler:
    # Note: We have not seen this in practice yet.
    if previous_int_handler is None:
      # According to the documentation, `None` indicates that the previous
      # signal handler was not installed by Python, and therefore we would
      # be unable to forward control to it from our signal handler.
      warnings.warn(
          'expected `signal.default_int_handler`, however found'
          ' a non-python-installed handler',
          RuntimeWarning,
      )
      return
    warnings.warn('expected `signal.default_int_handler', RuntimeWarning)
  # Assuming Python installed the previous SIGINT handler, it should be
  # safe to override the signal handler using C API.
  #
  # However, from practice, we know that Python does not "see" our SIGINT
  # handler. This means that while we don't break anything for Python --
  # which expects to own the current signal handler -- we may be overriding
  # another "stealthy" library's handler.
  try:
    unsafe_override_signal_handler_for_cancellation()
  except RuntimeWarning:
    pass


_load_ipython_extension_once = False


def load_ipython_extension(ipython):
  """Called when user runs: `%load_ext arolla.abc.cancellation`."""
  global _load_ipython_extension_once
  if _load_ipython_extension_once:
    # The extension is already loaded, possibly because of
    # the `_auto_load_ipython_extension` mechanism.
    return
  _load_ipython_extension_once = True
  if getattr(ipython, 'kernel', None):
    # Only register the hook if there is an `ipython.kernel`.
    ipython.events.register('pre_run_cell', _ipython_events_pre_run_cell_hook)
    # Run the hook without waiting for the next cell.
    _ipython_events_pre_run_cell_hook()


def unload_ipython_extension(ipython):
  """Called when ipython unloads this extension."""
  global _load_ipython_extension_once
  _load_ipython_extension_once = False
  try:
    ipython.events.unregister('pre_run_cell', _ipython_events_pre_run_cell_hook)
  except (ValueError, NameError):
    pass


def _auto_load_ipython_extension():
  if get_ipython := getattr(sys.modules.get('IPython'), 'get_ipython', None):
    if ipython := get_ipython():
      ipython.extension_manager.load_extension('arolla.abc.cancellation')


_auto_load_ipython_extension()
