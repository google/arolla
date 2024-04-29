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

"""Tests for arolla.py_utils."""

import gc
import sys
import uuid
import weakref

from absl.testing import absltest
from absl.testing import parameterized
from arolla.py_utils import testing_clib


class PyObjectAsCordTest(parameterized.TestCase):

  def test_pass_through(self):
    for _ in range(1024):
      expected_obj = uuid.uuid4()
      actual_obj = testing_clib.pass_object_through_cord(expected_obj)
      self.assertIs(actual_obj, expected_obj)


class PyObjectAsStatusPayloadTest(parameterized.TestCase):

  def test_pass_through(self):
    for _ in range(1024):
      expected_obj = uuid.uuid4()
      status = testing_clib.AbslStatus(
          testing_clib.ABSL_STATUS_CODE_ABORTED, ''
      )
      testing_clib.write_object_to_status_payload(
          status, 'rlv2.py.object.payload', expected_obj
      )
      actual_obj = testing_clib.read_object_to_status_from_status_payload(
          status, 'rlv2.py.object.payload'
      )
      self.assertIs(actual_obj, expected_obj)

  def test_set_payload_to_none(self):
    status = testing_clib.AbslStatus(testing_clib.ABSL_STATUS_CODE_ABORTED, '')
    testing_clib.write_object_to_status_payload(
        status, 'rlv2.py.object.payload', uuid.uuid4()
    )
    testing_clib.write_object_to_status_payload(
        status, 'rlv2.py.object.payload', None
    )
    self.assertEmpty(dict(status.AllPayloads()))

  def test_read_missing_payload(self):
    status = testing_clib.AbslStatus(testing_clib.ABSL_STATUS_CODE_ABORTED, '')
    actual_obj = testing_clib.read_object_to_status_from_status_payload(
        status, 'rlv2.py.object.payload'
    )
    self.assertIsNone(actual_obj)

  def test_token_structure(self):
    for _ in range(1024):
      status = testing_clib.AbslStatus(
          testing_clib.ABSL_STATUS_CODE_ABORTED, ''
      )
      testing_clib.write_object_to_status_payload(
          status, 'rlv2.py.object.payload', uuid.uuid4()
      )
      token = dict(status.AllPayloads())[b'rlv2.py.object.payload']
      self.assertRegex(
          token.decode(),
          '<py_object_as_cord:0x[0-9A-Fa-f]+:0x[0-9A-Fa-f]+:0x[0-9A-Fa-f]+>',
      )

  def test_ownership(self):
    obj = uuid.uuid4()
    weak_ref = weakref.ref(obj)
    status = testing_clib.AbslStatus(testing_clib.ABSL_STATUS_CODE_ABORTED, '')
    testing_clib.write_object_to_status_payload(
        status, 'rlv2.py.object.payload', obj
    )
    del obj
    gc.collect()
    self.assertIsNotNone(weak_ref())
    del status
    gc.collect()
    self.assertIsNone(weak_ref())

  def test_error_bad_token(self):
    status = testing_clib.AbslStatus(testing_clib.ABSL_STATUS_CODE_ABORTED, '')
    status.SetPayload(
        'rlv2.py.object.payload', '<py_object_as_cord:0x0:0x0:0x0>'
    )
    with self.assertRaises(ValueError):
      _ = testing_clib.read_object_to_status_from_status_payload(
          status, 'rlv2.py.object.payload'
      )

  def test_error_large_token(self):
    status = testing_clib.AbslStatus(testing_clib.ABSL_STATUS_CODE_ABORTED, '')
    status.SetPayload('rlv2.py.object.payload', ''.join(['x'] * 1024))
    with self.assertRaises(ValueError):
      _ = testing_clib.read_object_to_status_from_status_payload(
          status, 'rlv2.py.object.payload'
      )

  def test_error_token_deep_copy(self):
    obj = uuid.uuid4()
    status = testing_clib.AbslStatus(testing_clib.ABSL_STATUS_CODE_ABORTED, '')
    testing_clib.write_object_to_status_payload(
        status, 'rlv2.py.object.payload', obj
    )
    new_status = testing_clib.AbslStatus(
        testing_clib.ABSL_STATUS_CODE_ABORTED, ''
    )
    new_status.SetPayload(
        'rlv2.py.object.payload',
        dict(status.AllPayloads())[b'rlv2.py.object.payload'],
    )
    with self.assertRaises(ValueError):
      _ = testing_clib.read_object_to_status_from_status_payload(
          new_status, 'rlv2.py.object.payload'
      )


class SetPyErrFromStatusTest(parameterized.TestCase):

  @parameterized.parameters(range(3))
  def test_inlined(self, loc_count):
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

  def test_with_py_exception_payload(self):
    status = testing_clib.AbslStatus(
        testing_clib.ABSL_STATUS_CODE_FAILED_PRECONDITION, 'status-message'
    )
    exception = TypeError('exception-message')
    testing_clib.write_object_to_status_payload(
        status, testing_clib.PY_EXCEPTION, exception
    )
    raised_exception = None
    try:
      testing_clib.raise_from_status(status)
    except TypeError as ex:
      raised_exception = ex
    self.assertIsNotNone(raised_exception)
    self.assertIs(raised_exception, exception)

  def test_with_py_exception_cause(self):
    status = testing_clib.AbslStatus(
        testing_clib.ABSL_STATUS_CODE_FAILED_PRECONDITION, 'status-message'
    )
    exception = TypeError('exception-message')
    testing_clib.write_object_to_status_payload(
        status, testing_clib.PY_EXCEPTION_CAUSE, exception
    )
    raised_exception = None
    try:
      testing_clib.raise_from_status(status)
    except ValueError as ex:
      raised_exception = ex
    self.assertIsNotNone(raised_exception)
    self.assertIs(raised_exception.__cause__, exception)


class StatusCausedByPyErrTest(parameterized.TestCase):

  def test_status_ok(self):
    status = testing_clib.status_caused_by_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT, 'custom-message', None
    )
    self.assertTrue(status.ok())

  def test_status_non_ok(self):
    status = testing_clib.status_caused_by_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT,
        'custom-message',
        ValueError('py-error-message'),
    )
    self.assertEqual(
        status.code(), testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT
    )
    self.assertEqual(status.message(), 'custom-message')
    self.assertIn(testing_clib.PY_EXCEPTION_CAUSE, dict(status.AllPayloads()))

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
    status = testing_clib.status_with_raw_py_err(
        testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT,
        'custom-message',
        ValueError('py-error-message'),
    )
    self.assertEqual(
        status.code(), testing_clib.ABSL_STATUS_CODE_INVALID_ARGUMENT
    )
    self.assertEqual(status.message(), 'custom-message')
    self.assertIn(testing_clib.PY_EXCEPTION, dict(status.AllPayloads()))


class PyErrFormatFromCauseTest(parameterized.TestCase):

  def test_fixed(self):
    e3 = None
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


if __name__ == '__main__':
  absltest.main()
