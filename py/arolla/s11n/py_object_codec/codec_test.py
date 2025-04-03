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
import re
import sys

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.s11n.py_object_codec import codec_pb2 as _
from arolla.s11n.py_object_codec import testing_codecs
from arolla.s11n.testing import codec_test_case

# Import protobufs for the codecs in use:
from arolla.serialization_codecs.generic import scalar_codec_pb2 as _


JSON_PY_OBJECT_CODEC = arolla.s11n.register_py_object_codec(
    'JSonPyObjectCodec', testing_codecs.JSonPyObjectCodec
)

RAISING_PY_OBJECT_CODEC = arolla.s11n.register_py_object_codec(
    'RaisingPyObjectCodec', testing_codecs.RaisingPyObjectCodec
)


class PyObjectCodecTest(
    codec_test_case.S11nCodecTestCase, parameterized.TestCase
):

  def test_qtype_round_trip(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    self.assertDumpsEqual(arolla.abc.PY_OBJECT, text)
    self.assertLoadsEqual(text, arolla.abc.PY_OBJECT)

  def test_unsupported_py_obj_serialization(self):
    py_object_qvalue = arolla.abc.PyObject(object())
    with self.assertRaisesRegex(
        ValueError, 'missing serialization codec for PyObject'
    ):
      arolla.s11n.dumps(py_object_qvalue)

  def test_qvalue_round_trip(self):
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              data: "[123]"
              codec: "py_obj_codec:arolla.s11n.py_object_codec.registry.JSonPyObjectCodec"
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    py_object_qvalue = arolla.abc.PyObject([123], JSON_PY_OBJECT_CODEC)
    self.assertDumpsEqual(py_object_qvalue, text)
    loaded_py_object = self.parse_container_text_proto(text)
    self.assertIsInstance(loaded_py_object, arolla.abc.PyObject)
    self.assertEqual(loaded_py_object.py_value(), [123])
    self.assertEqual(loaded_py_object.codec(), JSON_PY_OBJECT_CODEC)

  def test_missing_extension(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          codec_index: 0
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError, re.escape('[NOT_FOUND] no extension found')
    ):
      self.parse_container_text_proto(text)

  def test_missing_value(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] { }
        }
      }
    """
    with self.assertRaisesRegex(ValueError, re.escape('missing value')):
      self.parse_container_text_proto(text)

  def test_missing_py_object_value_data(self):
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              codec: "py_obj_codec:arolla.s11n.py_object_codec.registry.JSonPyObjectCodec"
            }
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError, 'missing py_object.py_object_value.data; value=PY_OBJECT'
    ):
      self.parse_container_text_proto(text)

  def test_missing_serialization_codec(self):
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              data: "[123]"
            }
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError, 'missing py_object.py_object_value.codec; value=PY_OBJECT'
    ):
      self.parse_container_text_proto(text)

  def test_serialization_errors_are_propagated(self):
    py_object_qvalue = arolla.abc.PyObject([123], RAISING_PY_OBJECT_CODEC)
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'arolla.s11n.py_object_codec.tools.encode_py_object() failed;'
        " py_object_codec='py_obj_codec:arolla.s11n.py_object_codec.registry.RaisingPyObjectCodec'",
    ) as cm:
      arolla.s11n.dumps(py_object_qvalue)
    self.assertIsInstance(cm.exception.__cause__, RuntimeError)
    self.assertEqual(
        str(cm.exception.__cause__), 'a serialization error occurred'
    )

  def test_deserialization_errors_are_propagated(self):
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              data: "[123]"
              codec: "py_obj_codec:arolla.s11n.py_object_codec.registry.RaisingPyObjectCodec"
            }
          }
        }
      }
    """
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'arolla.abc.py_object_codec.tools.decode_py_object() failed;'
        " py_object_codec='py_obj_codec:arolla.s11n.py_object_codec.registry.RaisingPyObjectCodec';"
        ' value=PY_OBJECT;'
        ' detected_codec=arolla.python.PyObjectV1Proto.extension;'
        ' decoding_step.type=VALUE; while handling decoding_steps[1]',
    ) as cm:
      self.parse_container_text_proto(text)
    self.assertIsInstance(cm.exception.__cause__, RuntimeError)
    self.assertEqual(
        str(cm.exception.__cause__), 'a deserialization error occurred'
    )

  def test_error_encode_returns_non_bytes(self):
    class BadCodec(arolla.s11n.PyObjectCodecInterface):

      @classmethod
      def encode(cls, obj):
        del obj
        return object()

    codec = arolla.s11n.register_py_object_codec('BadCodec', BadCodec)
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'expected serialized object to be bytes, got object;'
        f' py_object_codec={codec.decode()!r}',
    ):
      arolla.s11n.dumps(arolla.abc.PyObject([123], codec))

  def test_error_decode_returns_non_qvalue(self):
    class BadCodec(arolla.s11n.PyObjectCodecInterface):

      @classmethod
      def decode(cls, obj, codec):
        del obj, codec
        return object()

    codec_str = arolla.s11n.register_py_object_codec(
        'BadCodec', BadCodec
    ).decode()
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value { data: "" codec: "%s" }
          }
        }
      }
    """ % codec_str
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected deserialized object to be arolla.abc.PyObject, got'
            f' object; py_object_codec={codec_str!r}; value=PY_OBJECT; '
        ),
    ):
      self.parse_container_text_proto(text)

  def test_error_decode_returns_non_py_object(self):
    class BadCodec(arolla.s11n.PyObjectCodecInterface):

      @classmethod
      def decode(cls, obj, codec):
        del obj, codec
        return arolla.int32(1)

    codec_str = arolla.s11n.register_py_object_codec(
        'BadCodec', BadCodec
    ).decode()
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value { data: "" codec: "%s" }
          }
        }
      }
    """ % codec_str
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected deserialized object to be PY_OBJECT, got INT32;'
            f' py_object_codec={codec_str!r}; value=PY_OBJECT; '
        ),
    ):
      self.parse_container_text_proto(text)

  def test_error_decode_returns_py_object_without_codec(self):
    class BadCodec(arolla.s11n.PyObjectCodecInterface):

      @classmethod
      def decode(cls, obj, codec):
        del obj, codec
        return arolla.abc.PyObject([123])

    codec_str = arolla.s11n.register_py_object_codec(
        'BadCodec', BadCodec
    ).decode()
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value { data: "" codec: "%s" }
          }
        }
      }
    """ % codec_str
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected deserialized object to have'
            f' py_object_codec={codec_str!r}, got no codec; value=PY_OBJECT; '
        ),
    ):
      self.parse_container_text_proto(text)

  def test_error_decode_returns_py_object_with_unexpected_codec(self):
    class BadCodec(arolla.s11n.PyObjectCodecInterface):

      @classmethod
      def decode(cls, obj, codec):
        del obj, codec
        return arolla.abc.PyObject([123], b'unexpected_codec')

    codec_str = arolla.s11n.register_py_object_codec(
        'BadCodec', BadCodec
    ).decode()
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value { data: "" codec: "%s" }
          }
        }
      }
    """ % codec_str
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected deserialized object to have'
            f" py_object_codec={codec_str!r}, got 'unexpected_codec';"
            ' value=PY_OBJECT; '
        ),
    ):
      self.parse_container_text_proto(text)

  def test_refcount(self):
    # Tests that we don't accidentally add or remove any obj references during
    # serialization and deserialization.
    class DummyCodec(arolla.s11n.PyObjectCodecInterface):

      @classmethod
      def encode(cls, obj):
        del obj
        return b'abc'

      @classmethod
      def decode(cls, serialized_obj, codec):
        del serialized_obj
        return arolla.abc.PyObject(object(), codec)

    codec = arolla.s11n.register_py_object_codec('DummyCodec', DummyCodec)

    obj = object()
    # Depending on the implementation, sys.getrefcount() might return 1 or 2
    # for the last reference.
    base_refcount = sys.getrefcount(obj)

    deserialized_obj = arolla.s11n.loads(
        arolla.s11n.dumps(arolla.abc.PyObject(obj, codec))
    ).py_value()  # pytype: disable=attribute-error
    gc.collect()
    self.assertEqual(sys.getrefcount(obj), base_refcount)
    self.assertEqual(sys.getrefcount(deserialized_obj), base_refcount)

  def test_e2e_pickle_py_object_codec(self):
    obj = arolla.abc.PyObject([123], arolla.s11n.PICKLE_PY_OBJECT_CODEC)
    deserialized_obj = arolla.s11n.loads(arolla.s11n.dumps(obj))
    self.assertEqual(obj.qtype, deserialized_obj.qtype)
    self.assertEqual(obj.py_value(), deserialized_obj.py_value())  # pytype: disable=attribute-error

  def test_e2e_reference_py_object_codec(self):
    codec = arolla.s11n.ReferencePyObjectCodec()
    obj = arolla.abc.PyObject([123], codec.name)
    deserialized_obj = arolla.s11n.loads(arolla.s11n.dumps(obj))
    self.assertEqual(obj.qtype, deserialized_obj.qtype)
    self.assertIs(obj.py_value(), deserialized_obj.py_value())  # pytype: disable=attribute-error


if __name__ == '__main__':
  absltest.main()
