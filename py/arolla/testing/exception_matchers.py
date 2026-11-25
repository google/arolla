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

"""Matchers and assertions for exceptions and their cause chains."""

import re
import textwrap
import traceback
import types
from typing import Any, Callable, Self, Sequence
from arolla.abc import abc as arolla_abc


def _flatten_cause_chain(exc_value: BaseException) -> list[BaseException]:
  """Returns `exc_value` followed by its causes; stops at a repeated cause."""
  # NOTE: Python allows cycles in the cause chain, e.g. `raise ex from ex`.
  if not isinstance(exc_value, BaseException):
    raise TypeError(
        'expected an exception, got:'
        f' {arolla_abc.get_type_name(type(exc_value))}'
    )
  cause_chain = []
  seen = set()
  curr_exc: BaseException | None = exc_value
  while curr_exc is not None and id(curr_exc) not in seen:
    seen.add(id(curr_exc))
    cause_chain.append(curr_exc)
    curr_exc = curr_exc.__cause__
  return cause_chain


ExcType = type[BaseException]
ExcPredicate = Callable[[BaseException], bool]
Regex = str | re.Pattern[str]
Regexes = list[Regex] | tuple[Regex, ...]
ExcPredicates = list[ExcPredicate] | tuple[ExcPredicate, ...]


def _get_notes(ex: BaseException) -> list[str]:
  """Returns the notes of `ex`; raises if they are malformed."""
  notes = getattr(ex, '__notes__', None)
  if notes is None:
    return []
  if not isinstance(notes, list):
    raise TypeError(
        'expected `__notes__` to be a list, got:'
        f' {arolla_abc.get_type_name(type(notes))}'
    )
  for i, note in enumerate(notes):
    if not isinstance(note, str):
      raise TypeError(
          f'expected `__notes__[{i}]` to be a str, got:'
          f' {arolla_abc.get_type_name(type(note))}'
      )
  return notes


def _check_exc_type(exc_type: Any) -> None:
  """Validates that `exc_type` is a valid exception type."""
  if isinstance(exc_type, type):
    if issubclass(exc_type, BaseException):
      return
    raise TypeError(
        'expected an exception type, got the class'
        f' {arolla_abc.get_type_name(exc_type)}'
    )
  type_name = arolla_abc.get_type_name(type(exc_type))
  if isinstance(exc_type, (str, re.Pattern)):
    raise TypeError(
        f'expected an exception type, got: {type_name}; to match only the'
        f' message, use message={exc_type!r}'
    )
  raise TypeError(f'expected an exception type, got: {type_name}')


def _check_regex(name: str, value: Any) -> None:
  """Validates that `value` is a valid regex."""
  if not isinstance(value, (str, re.Pattern)):
    raise TypeError(
        f'expected `{name}` to be a regex (str or re.Pattern), got:'
        f' {arolla_abc.get_type_name(type(value))}'
    )


def _check_predicate(name: str, value: Any) -> None:
  """Validates that `value` is a valid predicate."""
  if not callable(value):
    raise TypeError(
        f'expected `{name}` to be a predicate, got:'
        f' {arolla_abc.get_type_name(type(value))}'
    )
  # Calling an exception class constructs a truthy exception, so the predicate
  # would always hold.
  if isinstance(value, type) and issubclass(value, BaseException):
    raise TypeError(
        f'expected `{name}` to be a predicate, got the exception class'
        f' {value.__name__}; use exc({value.__name__})'
    )


def _as_list(
    name: str, value: Any, check_item: Callable[[str, Any], None]
) -> list[Any]:
  """Returns `value` as a list; requires a list or a tuple of valid items."""
  if not isinstance(value, (list, tuple)):
    raise TypeError(
        f'expected `{name}` to be a list or a tuple, got:'
        f' {arolla_abc.get_type_name(type(value))}'
    )
  for i, item in enumerate(value):
    check_item(f'{name}[{i}]', item)
  return list(value)


def _ordered_mismatch(
    name: str,
    items: Sequence[Any],
    patterns: Sequence[Any],
    matches: Callable[[Any, Any], Any],
    describe_item: Callable[[int], str],
) -> list[str]:
  """Describes the first pattern not matching distinct `items` in order."""
  # Greedily matching the earliest item for each pattern is optimal.
  start = 0
  for k, pattern in enumerate(patterns):
    for i in range(start, len(items)):
      if matches(pattern, items[i]):
        start = i + 1
        break
    else:
      where = (
          f' after {name}[{k - 1}] matched {describe_item(start - 1)}'
          if k
          else ''
      )
      return [f'{name}[{k}]={_field_repr(pattern)} not found{where}']
  return []


def _nested_mismatches(
    name: str, predicate: ExcPredicate, ex: BaseException
) -> list[str]:
  if isinstance(predicate, _ExcMatcher):
    return [f'{name}.{m}' for m in predicate.mismatches(ex)]
  return [] if predicate(ex) else [f'{name}={_field_repr(predicate)}']


class _ExcMatcher:
  """A predicate describing an expected exception; see `exc()`."""

  def __init__(
      self,
      exc_type: ExcType,
      *,
      message: Regex | None,
      notes: Regexes | None,
      root_cause: ExcPredicate | None,
      cause_chain: ExcPredicates | None,
  ):
    _check_exc_type(exc_type)
    if message is not None:
      _check_regex('message', message)
    if root_cause is not None:
      _check_predicate('root_cause', root_cause)
    self._exc_type = exc_type
    self._message = message
    self._notes = (
        None if notes is None else _as_list('notes', notes, _check_regex)
    )
    self._root_cause = root_cause
    self._cause_chain = (
        None
        if cause_chain is None
        else _as_list('cause_chain', cause_chain, _check_predicate)
    )

  def mismatches(self, ex: BaseException) -> list[str]:
    """Returns descriptions of the fields that `ex` doesn't match.

    A nested `exc(...)` matcher in `root_cause` reports its own mismatches
    with a `root_cause.` prefix; `notes` and `cause_chain` report the first
    item that couldn't be matched.

    Args:
      ex: The exception to check.
    """
    if not isinstance(ex, BaseException):
      raise TypeError(
          f'expected an exception, got: {arolla_abc.get_type_name(type(ex))}'
      )
    result = []
    if not isinstance(ex, self._exc_type):
      result.append(f'type={arolla_abc.get_type_name(self._exc_type)}')
    if self._message is not None and not re.search(self._message, str(ex)):
      result.append(f'message={_field_repr(self._message)}')
    if self._notes is not None:
      result += _ordered_mismatch(
          'notes',
          _get_notes(ex),
          self._notes,
          re.search,
          '__notes__[{}]'.format,
      )
    if self._root_cause is None and self._cause_chain is None:
      return result  # fast path
    chain = _flatten_cause_chain(ex)
    if self._root_cause is not None:
      result += _nested_mismatches('root_cause', self._root_cause, chain[-1])
    if self._cause_chain is not None:
      cause_chain = chain[1:]
      result += _ordered_mismatch(
          'cause_chain',
          cause_chain,
          self._cause_chain,
          lambda predicate, cause: predicate(cause),
          lambda i: (
              f'{arolla_abc.get_type_name(type(cause_chain[i]))} at depth'
              f' {i + 1}'
          ),
      )
    return result

  def __call__(self, ex: BaseException) -> bool:
    return not self.mismatches(ex)

  def __repr__(self) -> str:
    args = []
    if self._exc_type is not BaseException:
      args.append(arolla_abc.get_type_name(self._exc_type))
    for name, value in (
        ('message', self._message),
        ('notes', self._notes),
        ('root_cause', self._root_cause),
        ('cause_chain', self._cause_chain),
    ):
      if value is not None:
        args.append(f'{name}={_field_repr(value)}')
    return f'exc({", ".join(args)})'


def _field_repr(value: Any) -> str:
  if callable(value) and not isinstance(value, _ExcMatcher):
    return getattr(value, '__qualname__', repr(value))
  if isinstance(value, list):
    return '[' + ', '.join(map(_field_repr, value)) + ']'
  return repr(value)


def exc(
    exc_type: ExcType = BaseException,
    /,
    message: Regex | None = None,
    *,
    notes: Regexes | None = None,
    root_cause: ExcPredicate | None = None,
    cause_chain: ExcPredicates | None = None,
) -> ExcPredicate:
  """Returns a predicate describing an expected exception.

  The predicate holds if the exception matches all the specified fields.
  Only the `__cause__` chain is followed; `__context__` and the
  sub-exceptions of exception groups are not checked.

  Args:
    exc_type: The expected exception type (checked with `isinstance`).
    message: A regex searched (`re.search`) in the message, `str(ex)`. Use
      `re.escape()` for literal text. Can be passed positionally after
      `exc_type`.
    notes: A list of regexes; each regex must be found (`re.search`) in a
      distinct note of the exception (`__notes__`), in the given order. Other
      notes may appear before, between, and after the matching ones.
    root_cause: A predicate (e.g. `exc(...)`) for the root cause: the last
      exception in the `__cause__` chain, or the exception itself if it has no
      cause.
    cause_chain: A list of predicates (e.g. `exc(...)`) for the causes of the
      exception (`__cause__`, its `__cause__`, and so on; the exception itself
      is not included), ordered from the direct cause towards the root cause.
      Each predicate must match a distinct cause, and each matched cause must be
      a direct or indirect cause of the previously matched one; other causes may
      appear before, between, and after the matching ones.

  Returns:
    A predicate, compatible with
    `absltest.TestCase.assertRaisesWithPredicateMatch`.

  Example:
    with arolla.testing.assert_raises(
        ValueError,
        'python callable failed with RuntimeError',
        root_cause=arolla.testing.exc(RuntimeError, 'Boom!'),
    ):
      ...
  """
  return _ExcMatcher(
      exc_type,
      message=message,
      notes=notes,
      root_cause=root_cause,
      cause_chain=cause_chain,
  )


class AssertRaisesContext[E: BaseException]:
  """The context manager returned by `assert_raises`."""

  # The raised exception; available after the `with` block.
  exception: E

  def __init__(self, exc_type: type[E], matcher: _ExcMatcher):
    self._exc_type = exc_type
    self._matcher = matcher

  def __enter__(self) -> Self:
    return self

  # Note: The return type `bool` (rather than `bool | None`) tells static
  # analyzers that the context manager may suppress exceptions, so the code
  # after the `with` block is reachable.
  def __exit__(
      self,
      exc_type: type[BaseException] | None,
      exc_value: BaseException | None,
      tb: types.TracebackType | None,
  ) -> bool:
    del exc_type, tb  # Unused.
    if exc_value is None:
      raise AssertionError(
          f'{arolla_abc.get_type_name(self._exc_type)} not raised'
      )
    if not isinstance(exc_value, self._exc_type):
      return False  # Propagate the exception unchanged.
    self.exception = exc_value
    mismatches = self._matcher.mismatches(exc_value)
    if mismatches:
      if len(mismatches) == 1:
        details = f' {mismatches[0]}'
      else:
        details = ''.join(f'\n  {m}' for m in mismatches)
      raise AssertionError(
          f'Raised exception does not match:{details}\n'
          + _format_cause_chain(exc_value)
      ) from exc_value
    return True


def _format_exception_only(ex: BaseException) -> str:
  """Like `traceback.format_exception_only`, but without the notes."""
  # NOTE: For a `SyntaxError`, the location lines are moved after the message
  # line, so the message line always comes first.
  te = traceback.TracebackException(type(ex), ex, None, compact=True)
  te.__notes__ = None
  chunks = list(te.format_exception_only())
  if isinstance(ex, SyntaxError) and len(chunks) > 1:
    chunks = [chunks[-1], textwrap.dedent(''.join(chunks[:-1]))]
  return ''.join(chunks).rstrip('\n')


def _format_exception(ex: BaseException) -> str:
  """Formats `ex` and its notes; continuation lines are indented."""
  header, _, rest = _format_exception_only(ex).partition('\n')
  lines = [header]
  if rest:
    lines.append(textwrap.indent(rest, '  '))
  for note in _get_notes(ex):
    first, _, rest = note.rstrip('\n').partition('\n')
    lines.append(f'  note: {first}')
    if rest:
      lines.append(textwrap.indent(rest, '    '))
  return '\n'.join(lines)


def _format_cause_chain(ex: BaseException) -> str:
  """Formats `ex` followed by its causes, each one labeled."""
  return '\n'.join(
      f'{"caused by" if i else "Raised exception"}:\n'
      + textwrap.indent(_format_exception(cause), '  ')
      for i, cause in enumerate(_flatten_cause_chain(ex))
  )


def assert_raises[E: BaseException](
    exc_type: type[E],
    /,
    message: Regex | None = None,
    *,
    notes: Regexes | None = None,
    root_cause: ExcPredicate | None = None,
    cause_chain: ExcPredicates | None = None,
) -> AssertRaisesContext[E]:
  """Asserts that the `with` block raises an exception matching `exc(...)`.

  Exceptions that are not instances of `exc_type` are propagated unchanged.
  See `exc()` for the description of the arguments.

  Args:
    exc_type: The expected exception type.
    message: A regex searched in the message; can be passed positionally.
    notes: A list of regexes for the notes, in order.
    root_cause: A predicate for the root cause, or for the exception itself if
      it has no cause.
    cause_chain: A list of predicates for the causes, from the direct cause
      towards the root cause.

  Returns:
    A context manager whose `exception` attribute holds the raised exception
    after the `with` block.

  Example:
    with arolla.testing.assert_raises(
        ValueError,
        'python callable failed with RuntimeError',
        root_cause=arolla.testing.exc(RuntimeError, 'Boom!'),
    ) as cm:
      ...
  """
  return AssertRaisesContext(
      exc_type,
      _ExcMatcher(
          exc_type,
          message=message,
          notes=notes,
          root_cause=root_cause,
          cause_chain=cause_chain,
      ),
  )
