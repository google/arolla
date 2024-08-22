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

"""Test base for operators supported through multiple backends.

Operator tests can import this module, derive relevant test classes from
SelfEvalMixin, and replace `arolla.eval` with `self.eval` to enable the use of
different backends.
"""

import functools
import importlib
import re
from typing import Any, Callable

from absl import flags
from absl.testing import absltest
from arolla.operator_tests import backend_test_base_flags as _

_EVAL_FN_PATH = flags.DEFINE_string(
    'eval_fn',
    default=None,
    required=True,
    help='path to a callable with the same signature as `arolla.eval()`.',
)
_SKIP_TEST_PATTERNS = flags.DEFINE_multi_string(
    'skip_test_patterns',
    default=[],
    help='list of regex patterns for matching the beginning of the test method '
    'IDs of the form <test_class>.<test_method>. If any pattern matches a test '
    'ID, this test will be skipped. E.g. '
    r'[".*my_test_method", "^TestFoo\.test_bar$"] will 1) skip all test '
    'methods of all test classes where the method name contains the substring '
    '"my_test_method", 2) skip the specific test method "TestFoo.test_bar".',
)


def _load_function(function_path: str) -> Callable[..., Any]:
  """Returns a function from a module."""
  module_name, fn_name = function_path.rsplit('.', maxsplit=1)
  try:
    module = importlib.import_module(module_name)
  except ModuleNotFoundError as e:
    raise ImportError(f"Couldn't load function: {function_path!r}") from e
  fn = getattr(module, fn_name)
  if not callable(fn):
    raise TypeError(
        f'The object specified by {function_path!r} should be a callable, but '
        f'was actually a {type(fn)}.'
    )
  return fn


@functools.cache
def _get_eval_fn():
  """Returns `eval` function."""
  return _load_function(_EVAL_FN_PATH.value)


@functools.cache
def _get_skip_test_pattern() -> re.Pattern[str]:
  """Returns the compiled regex for _SKIP_TEST_PATTERNS."""
  skip_test_patterns = _SKIP_TEST_PATTERNS.value
  if skip_test_patterns:
    return re.compile('|'.join(skip_test_patterns))
  return re.compile(r'^\b\B$', re.ASCII)  # matches nothing


def _match_skip_test_pattern(test_id: str) -> bool:
  """Returns true iff the pattern matches some part of the test_id."""
  # The test id will be in the form <module>.<class>.<method>. The module is not
  # useful for the regex, so we discard it.
  _, cls_and_method = test_id.split('.', maxsplit=1)
  # Match the start of the string.
  return bool(_get_skip_test_pattern().match(cls_and_method))


class SelfEvalMixin(absltest.TestCase):
  r"""Mixin for operators tested through multiple backends.

  The mixin requires by default that each test calls `self.eval`. To disable
  this, set `self.require_self_eval_is_called = False` in the test function or
  in the setUp() method of the test class.

  To skip tests, set the `skip_test_patterns` flag to a list of regex-patterns
  indicating the tests to skip. These are matched with the beginning of the test
  ID in the form `<test_class>.<test_method>`. E.g.:
      skip_test_patterns = ['.*my_test_method', '^TestFoo\.test_bar$']
  will:
    1. Skip all test methods of all test classes where the method name contains
      the substring 'my_test_method'.
    2. Skip the specific test method 'TestFoo.test_bar'.
  Note that parameterized tests creates unique test IDs by appending a suffix to
  the base test name. To skip all parameterized tests from the base method
  `test_bar`, you must therefore omit '$' from your pattern.
  """

  def setUp(self):
    if _match_skip_test_pattern(self.id()):
      # This raises an error informing that this test should be skipped. The
      # tearDown() method is _not_ called in this case, so we check this before
      # any set up takes place.
      self.skipTest(f'test ID matches {_get_skip_test_pattern().pattern}')

    super().setUp()
    self._eval_called = False
    self._require_self_eval_is_called = True

  def tearDown(self):
    super().tearDown()
    if self.require_self_eval_is_called and not self._eval_called:
      raise AssertionError(
          '`self.eval` was not called. To disable this check, set '
          '`self.require_self_eval_is_called = False` in the test function or '
          'in the setUp() method of the test class.'
      )

  @property
  def require_self_eval_is_called(self) -> bool:
    return self._require_self_eval_is_called

  @require_self_eval_is_called.setter
  def require_self_eval_is_called(self, is_required: bool) -> None:
    if not isinstance(is_required, bool):
      raise TypeError('`require_self_eval_is_called` must be set to a bool.')
    self._require_self_eval_is_called = is_required

  def eval(self, *args: Any, **kwargs: Any) -> Any:
    self._eval_called = True
    eval_fn = _get_eval_fn()
    return eval_fn(*args, **kwargs)
