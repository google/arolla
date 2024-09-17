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

import inspect

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.s11n.testing import codec_test_case
from arolla.types.s11n import py_object_codec_pb2 as _
from arolla.types.s11n import py_object_s11n_test_helper

# Import protobufs for the codecs in use:
from arolla.serialization_codecs.generic import operator_codec_pb2 as _
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


def add(x, y=1.5):
  return x + y


class AddCodec(arolla.types.PyObjectCodecInterface):

  @classmethod
  def encode(cls, obj):
    assert obj is add
    return b'add_fn'

  @classmethod
  def decode(cls, serialized_obj):
    assert serialized_obj == b'add_fn'
    return add


ADD_CODEC = arolla.types.py_object_codec_str_from_class(AddCodec)
ADD_QTYPE = arolla.M.qtype.common_qtype(arolla.P.x, arolla.P.y)


class PyFunctionOperatorCodecTest(
    codec_test_case.S11nCodecTestCase, parameterized.TestCase
):

  def test_round_trip(self):
    text = """
      version: 2
      decoding_steps {  # [0]
        codec {
          name: "arolla.python.PyObjectV1Proto.extension"
        }
      }
      decoding_steps {  # [1]
        placeholder_node {
          placeholder_key: "x"
        }
      }
      decoding_steps {  # [2]
        placeholder_node {
          placeholder_key: "y"
        }
      }
      decoding_steps {  # [3]
        codec {
          name: "arolla.serialization_codecs.OperatorV1Proto.extension"
        }
      }
      decoding_steps {  # [4]
        value {
          codec_index: 3
          [arolla.serialization_codecs.OperatorV1Proto.extension] {
            registered_operator_name: "qtype.common_qtype"
          }
        }
      }
      decoding_steps {  # [5]
        operator_node {
          operator_value_index: 4
          input_expr_indices: 1
          input_expr_indices: 2
        }
      }
      decoding_steps {  # [6]
        value {
          codec_index: 0
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              codec: "%s"
              data: "add_fn"
            }
          }
        }
      }
      decoding_steps {  # [7]
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {  # [8]
        value {
          codec_index: 7
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            float32_value: 1.5
          }
        }
      }
      decoding_steps {  # [9]
        value {
          input_value_indices: 6
          input_value_indices: 8
          input_expr_indices: 5
          codec_index: 0
          [arolla.python.PyObjectV1Proto.extension] {
            py_function_operator_value {
              name: "test.add"
              signature_spec: "x, y="
              doc: "add docstring"
            }
          }
        }
      }
      decoding_steps {  # [10]
        output_value_index: 9
      }
    """ % ADD_CODEC.decode()
    value = arolla.types.PyFunctionOperator(
        'test.add',
        ('x, y=', 1.5),
        arolla.types.PyObject(add, codec=ADD_CODEC),
        qtype_inference_expr=ADD_QTYPE,
        doc='add docstring',
    )
    self.assertDumpsEqual(value, text)
    loaded_py_object = self.parse_container_text_proto(text)
    self.assertIsInstance(loaded_py_object, arolla.types.PyFunctionOperator)
    self.assertEqual(loaded_py_object.display_name, 'test.add')
    self.assertEqual(loaded_py_object.getdoc(), 'add docstring')
    self.assertEqual(
        inspect.signature(loaded_py_object), inspect.signature(add)
    )
    arolla.testing.assert_expr_equal_by_fingerprint(
        loaded_py_object.get_qtype_inference_expr(), ADD_QTYPE
    )
    self.assertIs(loaded_py_object.get_eval_fn().py_value(), add)

  def test_missing_eval_fn_serialization_codec(self):
    value = arolla.types.PyFunctionOperator(
        'test.add',
        'x, y',
        arolla.types.PyObject(add),
        qtype_inference_expr=ADD_QTYPE,
    )
    with self.assertRaisesRegex(
        ValueError,
        r'missing serialization codec for PyObject.*py_obj=PyEvalFn\(\); '
        r'value=PY_FUNCTION_OPERATOR with name=test\.add',
    ):
      arolla.s11n.dumps(value)

  def test_missing_name(self):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.python.PyObjectV1Proto.extension" }
        }
        decoding_steps {
          value {
            [arolla.python.PyObjectV1Proto.extension] {
              py_function_operator_value {
                signature_spec: "x, y="
                doc: "add docstring"
              }
            }
          }
        }
    """
    with self.assertRaisesRegex(
        ValueError, r'missing py_function_operator\.name'
    ):
      self.parse_container_text_proto(text)

  def test_missing_signature_spec(self):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.python.PyObjectV1Proto.extension" }
        }
        decoding_steps {
          value {
            [arolla.python.PyObjectV1Proto.extension] {
              py_function_operator_value {
                name: "test.add"
                doc: "add docstring"
              }
            }
          }
        }
    """
    with self.assertRaisesRegex(
        ValueError,
        r'missing py_function_operator\.signature_spec; '
        r'value=PY_FUNCTION_OPERATOR with name=test\.add',
    ):
      self.parse_container_text_proto(text)

  def test_too_few_values(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.python.PyObjectV1Proto.extension] {
            py_function_operator_value {
              name: "test.add"
              signature_spec: "x, y="
              doc: "add docstring"
            }
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        'expected at least one input_value_index, got 0; '
        r'value=PY_FUNCTION_OPERATOR with name=test\.add',
    ):
      self.parse_container_text_proto(text)

  def test_too_few_exprs(self):
    text = """
      version: 2
      decoding_steps {
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              codec: "%s"
              data: "add_fn"
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 1
          [arolla.python.PyObjectV1Proto.extension] {
            py_function_operator_value {
              name: "test.add"
              signature_spec: "x, y="
              doc: "add docstring"
            }
          }
        }
      }
    """ % ADD_CODEC.decode()
    with self.assertRaisesRegex(
        ValueError,
        'expected 1 input_expr_index, got 0; '
        r'value=PY_FUNCTION_OPERATOR with name=test\.add',
    ):
      self.parse_container_text_proto(text)

  def test_bad_signature(self):
    text = """
      version: 2
      decoding_steps {  # [0]
        codec { name: "arolla.python.PyObjectV1Proto.extension" }
      }
      decoding_steps {  # [1]
        codec {
          name: "arolla.serialization_codecs.OperatorV1Proto.extension"
        }
      }
      decoding_steps {  # [2]
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {  # [3]
        placeholder_node {
          placeholder_key: "x"
        }
      }
      decoding_steps {  # [4]
        placeholder_node {
          placeholder_key: "y"
        }
      }
      decoding_steps {  # [5]
        value {
          [arolla.serialization_codecs.OperatorV1Proto.extension] {
            registered_operator_name: "qtype.common_qtype"
          }
        }
      }
      decoding_steps {  # [6]
        operator_node {
          operator_value_index: 5
          input_expr_indices: 3
          input_expr_indices: 4
        }
      }
      decoding_steps {  # [7]
        value {
          [arolla.python.PyObjectV1Proto.extension] {
            py_object_value {
              codec: "%s"
              data: "add_fn"
            }
          }
        }
      }
      decoding_steps {  # [8]
        value {
          input_value_indices: 7
          input_expr_indices: 6
          [arolla.python.PyObjectV1Proto.extension] {
            py_function_operator_value {
              name: "test.add"
              signature_spec: "x, y="
              doc: "add docstring"
            }
          }
        }
      }
    """ % ADD_CODEC.decode()
    with self.assertRaisesRegex(
        ValueError,
        "default value expected, but not provided for parameter: 'y'",
    ):
      self.parse_container_text_proto(text)


if __name__ == '__main__':
  absltest.main()
