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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.s11n.testing import codec_test_case
from arolla.types.s11n import py_object_codec_pb2 as _
from arolla.types.s11n import py_object_s11n_test_helper

# Import protobufs for the codecs in use:
from arolla.serialization_codecs.generic import scalar_codec_pb2 as _


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
    self.assertDumpsEqual(arolla.types.PY_OBJECT, text)
    self.assertLoadsEqual(text, arolla.types.PY_OBJECT)

  def test_unsupported_py_obj_serialization(self):
    py_object_qvalue = arolla.types.PyObject(object())
    with self.assertRaisesRegex(
        ValueError, 'missing serialization codec for PyObject'
    ):
      arolla.s11n.dumps(py_object_qvalue)

  def test_qvalue_round_trip(self):
    codec = arolla.types.py_object_codec_str_from_class(
        py_object_s11n_test_helper.FooCodec
    )
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
              data: "{\"value\": 123}"
              codec: "%s"
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """ % codec.decode()
    py_object_qvalue = arolla.types.PyObject(
        py_object_s11n_test_helper.Foo(123), codec
    )
    self.assertDumpsEqual(py_object_qvalue, text)
    loaded_py_object = self.parse_container_text_proto(text)
    self.assertIsInstance(loaded_py_object, arolla.types.PyObject)
    self.assertIsInstance(
        loaded_py_object.py_value(), py_object_s11n_test_helper.Foo
    )
    self.assertEqual(loaded_py_object.py_value().value, 123)
    self.assertEqual(loaded_py_object.codec(), codec)

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
    with self.assertRaisesRegex(ValueError, 'missing value'):
      self.parse_container_text_proto(text)

  def test_missing_py_object_value_data(self):
    codec = arolla.types.py_object_codec_str_from_class(
        py_object_s11n_test_helper.FooCodec
    )
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              codec: "%s"
            }
          }
        }
      }
    """ % codec
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
              data: "{\"value\": 123}"
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
    codec = arolla.types.py_object_codec_str_from_class(
        py_object_s11n_test_helper.RaisingCodec
    )
    py_object_qvalue = arolla.types.PyObject(
        py_object_s11n_test_helper.Foo(123), codec
    )
    with self.assertRaisesRegex(ValueError, 'a serialization error occurred'):
      arolla.s11n.dumps(py_object_qvalue)

  def test_deserialization_errors_are_propagated(self):
    codec = arolla.types.py_object_codec_str_from_class(
        py_object_s11n_test_helper.RaisingCodec
    )
    text = r"""
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              data: "{\"value\": 123}"
              codec: "%s"
            }
          }
        }
      }
    """ % codec.decode()
    with self.assertRaisesRegex(ValueError, 'a deserialization error occurred'):
      self.parse_container_text_proto(text)


if __name__ == '__main__':
  absltest.main()
