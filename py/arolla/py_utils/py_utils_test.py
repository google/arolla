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

import gc
import signal
import sys
import threading
import time
import traceback

from absl.testing import absltest
from absl.testing import parameterized
from arolla.py_utils import testing_clib


class RestoreRaisedExceptionTest(parameterized.TestCase):

  def test_exception(self):
    original_ex = ValueError('test-error')
    try:
      testing_clib.restore_raised_exception(original_ex)
      self.fail('expected an exception')
    except ValueError as e:
      ex = e
    self.assertIs(ex, original_ex)

  def test_exception_ownership(self):
    ex = ValueError('test-error')
    gc.collect()
    original_refcount = sys.getrefcount(ex)
    try:
      testing_clib.restore_raised_exception(ex)
      self.fail('expected an exception')
    except ValueError:
      pass
    gc.collect()
    self.assertEqual(sys.getrefcount(ex), original_refcount)

  def test_traceback(self):
    try:
      raise ValueError('test-error')
    except ValueError as e:
      ex = e

    original_traceback = ex.__traceback__
    try:
      testing_clib.restore_raised_exception(ex)
    except ValueError as e:
      ex = e
    self.assertEqual(
        traceback.format_tb(ex.__traceback__)[1:],
        traceback.format_tb(original_traceback),
    )

  def test_traceback_ownership(self):
    try:
      raise ValueError('test-error')
    except ValueError as e:
      original_traceback = e.__traceback__
    gc.collect()
    original_traceback_refcount = sys.getrefcount(original_traceback)

    ex = ValueError('test-error')
    ex.__traceback__ = original_traceback
    try:
      testing_clib.restore_raised_exception(ex)
      self.fail('expected an exception')
    except ValueError:
      pass

    self.assertEqual(
        traceback.format_tb(ex.__traceback__)[1:],
        traceback.format_tb(original_traceback),
    )
    del ex

    gc.collect()
    self.assertEqual(
        sys.getrefcount(original_traceback), original_traceback_refcount
    )


class FetchRaisedExceptionTest(parameterized.TestCase):

  def test_exception(self):
    try:
      try:
        raise ValueError('test-cause-error')
      except ValueError as e:
        raise ValueError('test-error') from e
    except ValueError as e:
      original_ex = e

    original_cause_ex = original_ex.__cause__
    original_context_ex = original_ex.__context__
    original_traceback = original_ex.__traceback__

    gc.collect()
    original_ex_refcount = sys.getrefcount(original_ex)
    original_cause_ex_refcount = sys.getrefcount(original_cause_ex)
    original_context_ex_refcount = sys.getrefcount(original_context_ex)
    original_traceback_refcount = sys.getrefcount(original_traceback)

    ex = testing_clib.restore_and_fetch_raised_exception(original_ex)
    self.assertIs(ex, original_ex)
    self.assertIs(ex.__cause__, original_cause_ex)
    self.assertIs(ex.__context__, original_context_ex)
    self.assertIs(ex.__traceback__, original_traceback)

    del ex
    gc.collect()
    self.assertEqual(sys.getrefcount(original_ex), original_ex_refcount)
    self.assertEqual(
        sys.getrefcount(original_cause_ex), original_cause_ex_refcount
    )
    self.assertEqual(
        sys.getrefcount(original_context_ex), original_context_ex_refcount
    )
    self.assertEqual(
        sys.getrefcount(original_traceback), original_traceback_refcount
    )


class SetPyErrFromStatusTest(parameterized.TestCase):

  def test_inlined(self):
    # (the canonical error space, no message or payload)
    # See absl::Status::rep_ for details.
    with self.assertRaisesWithLiteralMatch(ValueError, ''):
      status = testing_clib.AbslStatus(
          testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT, ''
      )
      testing_clib.raise_from_status(status)
    with self.assertRaisesWithLiteralMatch(ValueError, '[NOT_FOUND]'):
      status = testing_clib.AbslStatus(
          testing_clib.ABSL_STATUS_CODE_NOT_FOUND, ''
      )
      testing_clib.raise_from_status(status)


class StatusCausedByPyErrTest(parameterized.TestCase):

  def test_status_ok(self):
    status = testing_clib.status_caused_by_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT, 'custom-message', None
    )
    self.assertTrue(status.ok())

  def test_status_non_ok(self):
    try:
      # Raise and catch the exception, so it gets a traceback.
      raise ValueError('py-error-message')
    except ValueError as e:
      original_cause = e
    original_traceback = original_cause.__traceback__
    self.assertIsNotNone(original_traceback)

    status = testing_clib.status_caused_by_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT,
        'custom-message',
        original_cause,
    )
    self.assertEqual(
        status.code(), testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT
    )
    self.assertEqual(status.message(), 'custom-message')

    with self.assertRaises(ValueError) as cm:
      testing_clib.raise_from_status(status)
    self.assertRegex(str(cm.exception), 'custom-message')
    self.assertIs(cm.exception.__cause__, original_cause)
    self.assertIs(cm.exception.__context__, original_cause)
    self.assertTrue(cm.exception.__suppress_context__)

    self.assertIsNone(cm.exception.__traceback__)
    self.assertIs(cm.exception.__cause__.__traceback__, original_traceback)  # pytype: disable=attribute-error

  def test_double_nesting(self):
    exc = ValueError('py-error-message')
    status = testing_clib.status_caused_by_status_caused_by_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT,
        'custom-message',
        'cause-message',
        exc,
    )
    self.assertEqual(
        status.code(), testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT
    )
    self.assertEqual(status.message(), 'custom-message')

    with self.assertRaises(ValueError) as cm:
      testing_clib.raise_from_status(status)
    self.assertRegex(str(cm.exception), 'custom-message')
    self.assertRegex(str(cm.exception.__cause__), 'cause-message')
    self.assertIsNotNone(cm.exception.__cause__)
    self.assertIs(cm.exception.__cause__.__cause__, exc)  # pytype: disable=attribute-error

  def test_ownership_regression(self):
    # NOTE: We use the raw refcount in this test because weakref cannot handle
    # ValueError.
    ex = ValueError('py-error-message')
    gc.collect()
    refcount = sys.getrefcount(ex)
    testing_clib.status_caused_by_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT, 'custom-message', ex
    )
    gc.collect()
    self.assertEqual(sys.getrefcount(ex), refcount)


class StatusWithRawPyErrTest(parameterized.TestCase):

  def test_status_ok(self):
    status = testing_clib.status_with_raw_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT, 'custom-message', None
    )
    self.assertTrue(status.ok())

  def test_status_non_ok(self):
    try:
      # Raise and catch the exception, so it gets a traceback.
      raise ValueError('py-error-message')
    except ValueError as e:
      original_exc = e
    original_traceback = original_exc.__traceback__
    self.assertIsNotNone(original_traceback)

    status = testing_clib.status_with_raw_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT,
        'custom-message',
        original_exc,
    )
    self.assertEqual(
        status.code(), testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT
    )
    self.assertEqual(status.message(), 'custom-message')

    # assertRaises with a context manager clears traceback of the caught
    # exception, so we use a try-except block instead.
    try:
      testing_clib.raise_from_status(status)
    except ValueError as e:
      raised_exc = e
    self.assertIs(raised_exc, original_exc)
    self.assertIsNone(raised_exc.__cause__)
    self.assertIsNone(raised_exc.__context__)
    self.assertFalse(raised_exc.__suppress_context__)
    self.assertEqual(
        # raise_from_status adds one more frame to the traceback, so we skip it.
        traceback.extract_tb(raised_exc.__traceback__)[1:],
        traceback.extract_tb(original_traceback),
    )


class PyErrFormatFromCauseTest(parameterized.TestCase):

  def test_fixed(self):
    try:
      _ = testing_clib.call_format_from_cause()
      self.fail('expected an exception')
    except AssertionError as ex:
      e3 = ex
    self.assertIsInstance(e3, AssertionError)
    self.assertEqual(str(e3), 'third error')
    e2 = e3.__cause__
    self.assertIs(e3.__context__, e2)
    self.assertIsInstance(e2, TypeError)
    self.assertEqual(str(e2), 'second error')
    e1 = e2.__cause__
    self.assertIs(e2.__context__, e1)
    self.assertIsInstance(e1, ValueError)
    self.assertEqual(str(e1), 'first error')
    self.assertIsNone(e1.__cause__)
    self.assertIsNone(e1.__context__)


class MemberUtilsTest(parameterized.TestCase):

  def test_lookup_type_member(self):
    class T:

      @property
      def attr1(self):
        pass

      @classmethod
      def attr2(cls):
        pass

      @staticmethod
      def attr3():
        pass

      attr4 = object()

    for k, v in vars(T).items():
      self.assertIs(testing_clib.lookup_type_member(T, k), v)
    self.assertIsNone(testing_clib.lookup_type_member(T, 'attr5'), None)

  def testBindMember(self):
    class T:

      @property
      def attr1(self):
        return ('attr1', (self,))

      @classmethod
      def attr2(cls, *args, **kwargs):
        return ('attr2', (cls,) + args, tuple(kwargs.items()))

      @staticmethod
      def attr3(*args, **kwargs):
        return ('attr3', args, tuple(kwargs.items()))

      def attr4(self, *args, **kwargs):
        return ('attr4', (self,) + args, tuple(kwargs.items()))

      attr5 = object()

    members = vars(T)
    t = T()
    self.assertEqual(
        testing_clib.bind_member(members['attr1'], t),
        ('attr1', (t,)),
    )
    self.assertEqual(
        testing_clib.bind_member(members['attr2'], t)(1, 2, u='x', v='y'),
        ('attr2', (T, 1, 2), (('u', 'x'), ('v', 'y'))),
    )
    self.assertEqual(
        testing_clib.bind_member(members['attr3'], t)(1, 2, u='x', v='y'),
        ('attr3', (1, 2), (('u', 'x'), ('v', 'y'))),
    )
    self.assertEqual(
        testing_clib.bind_member(members['attr4'], t)(1, 2, u='x', v='y'),
        ('attr4', (t, 1, 2), (('u', 'x'), ('v', 'y'))),
    )
    self.assertIs(
        testing_clib.bind_member(members['attr5'], t),
        members['attr5'],
    )

  def test_call_member(self):
    class T:

      @property
      def attr1(self):
        def impl(*args, **kwargs):
          return ('attr1', self, args, tuple(kwargs.items()))

        return impl

      @classmethod
      def attr2(cls, *args, **kwargs):
        return ('attr2', cls, args, tuple(kwargs.items()))

      @staticmethod
      def attr3(*args, **kwargs):
        return ('attr3', args, tuple(kwargs.items()))

      def attr4(self, *args, **kwargs):
        return ('attr4', self, args, tuple(kwargs.items()))

    members = vars(T)
    t = T()
    self.assertEqual(
        testing_clib.call_member(members['attr1'], t, (1,), dict(w=2)),
        t.attr1(1, w=2),
    )
    self.assertEqual(
        testing_clib.call_member(members['attr2'], t, (1,), dict(w=2)),
        t.attr2(1, w=2),
    )
    self.assertEqual(
        testing_clib.call_member(members['attr3'], t, (1,), dict(w=2)),
        t.attr3(1, w=2),
    )
    self.assertEqual(
        testing_clib.call_member(members['attr4'], t, (1,), dict(w=2)),
        t.attr4(1, w=2),
    )

  def test_vectorcall_member(self):
    class T:

      @property
      def attr1(self):
        def impl(*args, **kwargs):
          return ('attr1', self, args, tuple(kwargs.items()))

        return impl

      @classmethod
      def attr2(cls, *args, **kwargs):
        return ('attr2', cls, args, tuple(kwargs.items()))

      @staticmethod
      def attr3(*args, **kwargs):
        return ('attr3', args, tuple(kwargs.items()))

      def attr4(self, *args, **kwargs):
        return ('attr4', self, args, tuple(kwargs.items()))

    members = vars(T)
    t = T()
    self.assertEqual(
        testing_clib.vectorcall_member(members['attr1'], [t, 1, 2], 2, ('w',)),
        t.attr1(1, w=2),
    )
    self.assertEqual(
        testing_clib.vectorcall_member(members['attr2'], [t, 1, 2], 2, ('w',)),
        t.attr2(1, w=2),
    )
    self.assertEqual(
        testing_clib.vectorcall_member(members['attr3'], [t, 1, 2], 2, ('w',)),
        t.attr3(1, w=2),
    )
    self.assertEqual(
        testing_clib.vectorcall_member(members['attr4'], [t, 1, 2], 2, ('w',)),
        t.attr4(1, w=2),
    )
    self.assertEqual(
        testing_clib.vectorcall_member(members['attr4'], [t, 1, 2], 3, None),
        t.attr4(1, 2),
    )
    with self.assertRaisesWithLiteralMatch(TypeError, 'no arguments provided'):
      _ = testing_clib.vectorcall_member(members['attr4'], [], 0, None)


class PyCancellationScopeTest(parameterized.TestCase):

  def test_cancelled_by_signal(self):
    def send_sigint():
      time.sleep(0.1)
      signal.raise_signal(signal.SIGINT)

    threading.Thread(target=send_sigint).start()
    with self.assertRaisesWithLiteralMatch(
        ValueError, '[CANCELLED] interrupted'
    ):
      testing_clib.wait_in_cancellation_scope(0.5)
    # No secondary cancellations.
    testing_clib.wait_in_cancellation_scope(0.01)  # no exception
    testing_clib.wait_in_cancellation_scope(0.01)  # no exception

  def test_manual_cancellation(self):
    for _ in range(100):
      self.assertTrue(testing_clib.check_manual_cancellation())

  def test_non_main_thread_behaviour(self):
    thread = threading.Thread(
        target=lambda: self.assertFalse(
            testing_clib.check_manual_cancellation()
        )
    )
    thread.start()
    thread.join()


if __name__ == '__main__':
  absltest.main()
