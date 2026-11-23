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

import inspect
import re
import typing

from absl.testing import absltest
from absl.testing import parameterized
from arolla.testing import exception_matchers

exc = exception_matchers.exc


def _never(ex):
  del ex
  return False


def _chain(*excs: BaseException) -> BaseException:
  """Links `excs` into a `__cause__` chain, outermost first; returns excs[0]."""
  for outer, inner in zip(excs, excs[1:]):
    outer.__cause__ = inner
  return excs[0]


def _noted(ex: BaseException, *notes: str) -> BaseException:
  """Adds `notes` to `ex`; returns `ex`."""
  for note in notes:
    ex.add_note(note)
  return ex


class _CustomError(Exception):
  pass


class ExceptionMatchersTest(parameterized.TestCase):

  def test_flatten_cause_chain(self):
    exc1 = ValueError('foo')
    exc2 = ValueError('bar')
    exc2.__cause__ = exc1
    self.assertEqual(
        exception_matchers._flatten_cause_chain(exc2), [exc2, exc1]
    )

    with self.assertRaisesRegex(TypeError, 'expected an exception'):
      exception_matchers._flatten_cause_chain(1)  # pyrefly: ignore[bad-argument-type]

  def test_flatten_cause_chain_with_cycle(self):
    exc1 = ValueError('foo')
    exc1.__cause__ = exc1
    self.assertEqual(exception_matchers._flatten_cause_chain(exc1), [exc1])

    exc2 = ValueError('bar')
    exc3 = ValueError('baz')
    exc2.__cause__ = exc3
    exc3.__cause__ = exc2
    self.assertEqual(
        exception_matchers._flatten_cause_chain(exc2), [exc2, exc3]
    )

  def test_any_cause_message_regex(self):
    predicate = exception_matchers.any_cause_message_regex(r'foo')

    with self.assertRaisesRegex(TypeError, 'expected an exception'):
      predicate(1)  # pyrefly: ignore[bad-argument-type]

    exc_value = ValueError('bar')
    self.assertFalse(predicate(exc_value))

    exc_value = ValueError('bar')
    exc_value.__cause__ = ValueError('bar')
    self.assertFalse(predicate(exc_value))

    exc_value = ValueError('foo')
    self.assertTrue(predicate(exc_value))

    exc_value = ValueError('bar')
    exc_value.__cause__ = ValueError('foo')
    self.assertTrue(predicate(exc_value))

    exc_value = ValueError('bar')
    exc_value.__cause__ = ValueError('baz')
    exc_value.__cause__.__cause__ = ValueError('foo')
    self.assertTrue(predicate(exc_value))

  def test_any_note_regex(self):
    predicate = exception_matchers.any_note_regex(r'foo')

    with self.assertRaisesRegex(TypeError, 'expected an exception'):
      predicate(1)  # pyrefly: ignore[bad-argument-type]

    exc_value = ValueError('bar')
    self.assertFalse(predicate(exc_value))

    exc_value = ValueError('bar')
    exc_value.add_note('bar')
    self.assertFalse(predicate(exc_value))

    exc_value = ValueError('foo')
    self.assertFalse(predicate(exc_value))

    exc_value = ValueError('bar')
    exc_value.add_note('foo')
    self.assertTrue(predicate(exc_value))

    exc_value = ValueError('bar')
    exc_value.__cause__ = ValueError('baz')
    exc_value.__cause__.add_note('foo')
    self.assertFalse(predicate(exc_value))

    exc_value = ValueError('bar')
    exc_value.add_note('abc foo baz')
    self.assertTrue(predicate(exc_value))

  def test_exc_type(self):
    self.assertTrue(exc()(ValueError()))
    self.assertTrue(exc(ValueError)(ValueError()))
    self.assertTrue(exc(Exception)(ValueError()))
    self.assertFalse(exc(TypeError)(ValueError()))
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected an exception, got: int'
    ):
      exc()(1)  # pyrefly: ignore[bad-argument-type]

  @parameterized.parameters(
      (
          ('foo',),
          {},
          (
              'expected an exception type, got: str; to match only the'
              " message, use message='foo'"
          ),
      ),
      (
          (re.compile('foo'),),
          {},
          (
              'expected an exception type, got: re.Pattern; to match only the'
              " message, use message=re.compile('foo')"
          ),
      ),
      (
          ((KeyError, ValueError),),  # Tuples of types are not supported.
          {},
          'expected an exception type, got: tuple',
      ),
      (
          (int,),
          {},
          'expected an exception type, got the class int',
      ),
      (
          (ValueError, 1),
          {},
          'expected `message` to be a regex (str or re.Pattern), got: int',
      ),
      (
          (),
          {'notes': 'a'},
          'expected `notes` to be a list or a tuple, got: str',
      ),
      (
          (),
          {'notes': {'a'}},
          'expected `notes` to be a list or a tuple, got: set',
      ),
      (
          (),
          {'notes': ['a', 1]},
          'expected `notes[1]` to be a regex (str or re.Pattern), got: int',
      ),
      (
          (),
          {'root_cause': RuntimeError},
          (
              'expected `root_cause` to be a predicate, got the exception'
              ' class RuntimeError; use exc(RuntimeError)'
          ),
      ),
      (
          (),
          {'cause_chain': exc()},
          (
              'expected `cause_chain` to be a list or a tuple, got:'
              ' arolla.testing.exception_matchers._ExcMatcher'
          ),
      ),
      (
          (),
          {'cause_chain': [exc(), KeyError]},
          (
              'expected `cause_chain[1]` to be a predicate, got the exception'
              ' class KeyError; use exc(KeyError)'
          ),
      ),
  )
  def test_exc_rejects_invalid_arguments(self, args, kwargs, expected_error):
    with self.assertRaisesWithLiteralMatch(TypeError, expected_error):
      exc(*args, **kwargs)

  def test_exc_message(self):
    predicate = exc(message='fo+')
    self.assertTrue(predicate(ValueError('fooo')))
    self.assertTrue(predicate(ValueError('abc foo')))  # re.search.
    self.assertFalse(predicate(ValueError('bar')))
    # Causes and notes are not checked.
    self.assertFalse(predicate(_chain(ValueError('bar'), ValueError('foo'))))
    self.assertFalse(predicate(_noted(ValueError('bar'), 'foo')))

    self.assertTrue(exc(message=re.compile('fo+'))(ValueError('fo')))
    self.assertTrue(exc(ValueError, 'fo+')(ValueError('foo')))  # Positional.
    self.assertFalse(exc(ValueError, 'fo+')(ValueError('bar')))

  def test_exc_notes(self):
    self.assertTrue(exc(notes=[])(ValueError('foo')))
    self.assertFalse(exc(notes=['foo'])(ValueError('foo')))

    def matches(notes):
      return exc(notes=notes)(
          _noted(ValueError('foo'), 'note a', 'note b', 'note c')
      )

    self.assertTrue(matches(['b']))
    self.assertTrue(matches([re.compile('b')]))
    self.assertTrue(matches(('a', 'c')))
    self.assertTrue(matches(['a', 'b', 'c']))
    self.assertTrue(matches(['note', 'note']))
    self.assertFalse(matches(['d']))
    self.assertFalse(matches(['c', 'a']))  # Order.
    self.assertFalse(matches(['b', 'b']))  # Distinct notes.
    self.assertFalse(matches(['foo']))  # The message is not checked.

    self.assertFalse(
        exc(notes=['foo'])(
            _chain(ValueError('bar'), _noted(ValueError('baz'), 'foo'))
        )
    )

  def test_exc_root_cause(self):
    predicate = exc(root_cause=exc(TypeError, 'foo'))
    self.assertTrue(predicate(TypeError('foo')))
    self.assertFalse(predicate(ValueError('foo')))
    self.assertTrue(predicate(_chain(ValueError('bar'), TypeError('foo'))))
    self.assertTrue(
        predicate(
            _chain(ValueError('bar'), ValueError('baz'), TypeError('foo'))
        )
    )
    self.assertFalse(
        predicate(
            _chain(ValueError('bar'), TypeError('foo'), ValueError('baz'))
        )
    )
    # Only `__cause__` is followed.
    exc_value = ValueError('bar')
    exc_value.__context__ = TypeError('foo')
    self.assertFalse(predicate(exc_value))

  def test_exc_cause_chain(self):
    exc_value = _chain(
        ValueError('top'), ValueError('a'), ValueError('b'), TypeError('c')
    )

    def matches(*messages):
      return exc(cause_chain=[exc(message=m) for m in messages])(exc_value)

    self.assertTrue(matches())
    self.assertTrue(matches('a'))
    self.assertTrue(matches('b'))
    self.assertTrue(matches('c'))
    self.assertTrue(matches('a', 'b'))
    self.assertTrue(matches('a', 'c'))  # An indirect cause.
    self.assertTrue(matches('b', 'c'))
    self.assertTrue(matches('a', 'b', 'c'))
    self.assertFalse(matches('top'))  # The exception itself is not a cause.
    self.assertFalse(matches('d'))
    self.assertFalse(matches('b', 'a'))  # Order.
    self.assertFalse(matches('a', 'a'))  # Distinct exceptions.
    self.assertFalse(matches('a', 'b', 'c', 'c'))
    self.assertTrue(
        exc(
            cause_chain=[
                exc(message='[ab]'),
                exc(message='[bc]'),
                exc(TypeError),
            ]
        )(exc_value)
    )

  def test_exc_combined(self):
    predicate = exc(
        ValueError,
        'python callable failed',
        notes=['operator_name: py.call'],
        root_cause=exc(RuntimeError, 'Boom!', notes=['calling fn']),
        cause_chain=[exc(RuntimeError, notes=['calling fn'])],
    )
    self.assertTrue(
        predicate(
            _chain(
                _noted(
                    ValueError('python callable failed'),
                    'operator_name: py.call',
                ),
                _noted(RuntimeError('Boom!'), 'while calling fn'),
            )
        )
    )

  @parameterized.parameters(
      (exc(ValueError, 'foo'), []),
      (exc(TypeError, 'bar'), ['type=TypeError', "message='bar'"]),
      (exc(notes=['c']), ["notes[0]='c' not found"]),
      (
          exc(notes=['b', 'a']),
          ["notes[1]='a' not found after notes[0] matched __notes__[1]"],
      ),
      (
          exc(root_cause=exc(TypeError, 'Bam')),
          ['root_cause.type=TypeError', "root_cause.message='Bam'"],
      ),
      (
          exc(root_cause=exc(root_cause=exc(KeyError))),
          ['root_cause.root_cause.type=KeyError'],
      ),
      (exc(root_cause=_never), [f'root_cause={_never.__qualname__}']),
      (
          exc(cause_chain=[exc(ValueError)]),  # Not the exception itself.
          ['cause_chain[0]=exc(ValueError) not found'],
      ),
      (
          exc(cause_chain=[exc(RuntimeError), exc(KeyError)]),
          [
              'cause_chain[1]=exc(KeyError) not found after cause_chain[0]'
              ' matched RuntimeError at depth 1'
          ],
      ),
  )
  def test_exc_mismatches(self, exc_matcher, expected_mismatches):
    ex = _chain(
        _noted(ValueError('foo'), 'note a', 'note b'), RuntimeError('Boom!')
    )
    self.assertIsInstance(exc_matcher, exception_matchers._ExcMatcher)
    self.assertEqual(exc_matcher.mismatches(ex), expected_mismatches)

  def test_exc_mismatches_without_cause(self):
    exc_matcher = exc(cause_chain=[exc()])
    self.assertIsInstance(exc_matcher, exception_matchers._ExcMatcher)
    self.assertEqual(
        exc_matcher.mismatches(RuntimeError('Boom!')),
        ['cause_chain[0]=exc() not found'],
    )
    exc_matcher = exc(root_cause=exc(TypeError, 'Bam'))
    self.assertIsInstance(exc_matcher, exception_matchers._ExcMatcher)
    self.assertEqual(
        exc_matcher.mismatches(RuntimeError('Boom!')),
        ['root_cause.type=TypeError', "root_cause.message='Bam'"],
    )

  def test_exc_mismatches_qualified_type_name(self):
    exc_matcher = exc(root_cause=exc(_CustomError))
    self.assertIsInstance(exc_matcher, exception_matchers._ExcMatcher)
    self.assertEqual(
        exc_matcher.mismatches(_chain(ValueError('foo'), KeyError('bar'))),
        [f'root_cause.type={__name__}._CustomError'],
    )

  def test_exc_cyclic_cause_chain(self):
    exc_value = ValueError('foo')
    exc_value.__cause__ = exc_value
    self.assertTrue(exc(ValueError, 'foo')(exc_value))
    # The repeated exception is not followed again: the chain is `[exc_value]`.
    self.assertTrue(exc(root_cause=exc(ValueError))(exc_value))
    self.assertFalse(exc(cause_chain=[exc(ValueError)])(exc_value))

  def test_exc_malformed_notes(self):
    exc_value = ValueError('foo')
    exc_value.__notes__ = 'abc'  # pyrefly: ignore[bad-assignment]
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected `__notes__` to be a list, got: str'
    ):
      exc(notes=['a'])(exc_value)

    exc_value.__notes__ = ['a', 1]  # pyrefly: ignore[bad-assignment]
    with self.assertRaisesWithLiteralMatch(
        TypeError, 'expected `__notes__[1]` to be a str, got: int'
    ):
      exc(notes=['a'])(exc_value)

    # Notes are not inspected unless requested.
    self.assertTrue(exc(ValueError, 'foo')(exc_value))

  def test_exc_repr(self):
    self.assertEqual(repr(exc()), 'exc()')
    self.assertEqual(
        repr(
            exc(
                ValueError,
                re.compile('ba+r', re.IGNORECASE),
                notes=['a', re.compile('b')],
                root_cause=exc(TypeError, notes=['d']),
                cause_chain=[exc(message='e')],
            )
        ),
        "exc(ValueError, message=re.compile('ba+r', re.IGNORECASE),"
        " notes=['a', re.compile('b')],"
        " root_cause=exc(TypeError, notes=['d']),"
        " cause_chain=[exc(message='e')])",
    )
    self.assertEqual(repr(exc(_CustomError)), f'exc({__name__}._CustomError)')

  def test_assert_raises(self):
    with exception_matchers.assert_raises(KeyError) as cm:
      raise KeyError('foo')
    typing.assert_type(cm.exception, KeyError)  # check the static type
    self.assertIsInstance(cm.exception, KeyError)
    # The traceback starts at the `with` statement, not in `assert_raises`.
    tb = cm.exception.__traceback__
    self.assertIsNotNone(tb)
    self.assertIs(tb.tb_frame, inspect.currentframe())

    with exception_matchers.assert_raises(
        ValueError, 'fo+', root_cause=exc(TypeError, 'bar')
    ):
      raise ValueError('foo') from TypeError('bar')

  def test_assert_raises_mismatch(self):
    with self.assertRaises(AssertionError) as cm:
      with exception_matchers.assert_raises(ValueError, 'bar'):
        raise ValueError('foo')
    self.assertEqual(
        str(cm.exception),
        "Raised exception does not match: message='bar'\n"
        'Raised exception:\n'
        '  ValueError: foo',
    )
    self.assertIsInstance(cm.exception.__cause__, ValueError)

  def test_assert_raises_mismatch_multiple(self):
    with self.assertRaises(AssertionError) as cm:
      with exception_matchers.assert_raises(
          ValueError,
          notes=['operator_name: kd.call'],
          root_cause=exc(TypeError, 'Boom!'),
      ):
        raise _chain(
            _noted(
                ValueError('python callable failed\n\nwith RuntimeError'),
                'operator_name: py.call',
                'multi-line\nnote',
            ),
            _noted(
                RuntimeError('Boom!'),
                'Error occurred during evaluation of kd.apply_py',
            ),
        )
    self.assertEqual(
        str(cm.exception),
        'Raised exception does not match:\n'
        "  notes[0]='operator_name: kd.call' not found\n"
        '  root_cause.type=TypeError\n'
        'Raised exception:\n'
        '  ValueError: python callable failed\n'
        '\n'
        '    with RuntimeError\n'
        '    note: operator_name: py.call\n'
        '    note: multi-line\n'
        '      note\n'
        'caused by:\n'
        '  RuntimeError: Boom!\n'
        '    note: Error occurred during evaluation of kd.apply_py',
    )

  def test_assert_raises_mismatch_syntax_error(self):
    with self.assertRaises(AssertionError) as cm:
      with exception_matchers.assert_raises(SyntaxError, 'foo'):
        raise SyntaxError('bar', ('file.py', 1, 5, 'x = (\n', 1, 6))
    self.assertEqual(
        str(cm.exception),
        "Raised exception does not match: message='foo'\n"
        'Raised exception:\n'
        '  SyntaxError: bar\n'
        '    File "file.py", line 1\n'
        '      x = (\n'
        '          ^',
    )

  def test_assert_raises_mismatch_cyclic_cause_chain(self):
    exc_value = ValueError('foo')
    exc_value.__cause__ = exc_value
    with self.assertRaises(AssertionError) as cm:
      with exception_matchers.assert_raises(ValueError, 'bar'):
        raise exc_value
    self.assertEqual(
        str(cm.exception),
        "Raised exception does not match: message='bar'\n"
        'Raised exception:\n'
        '  ValueError: foo',
    )

  def test_assert_raises_not_raised(self):
    with self.assertRaisesWithLiteralMatch(
        AssertionError, 'ValueError not raised'
    ):
      with exception_matchers.assert_raises(ValueError):
        pass

  def test_assert_raises_other_exception(self):
    context = exception_matchers.assert_raises(ValueError)
    with self.assertRaisesWithLiteralMatch(TypeError, 'foo'):
      with context:
        raise TypeError('foo')

  def test_assert_raises_with_predicate_match(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError, exc(root_cause=exc(TypeError, 'bar'))
    ):
      raise ValueError('foo') from TypeError('bar')


if __name__ == '__main__':
  absltest.main()
