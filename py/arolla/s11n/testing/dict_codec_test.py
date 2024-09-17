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

import re

from absl.testing import absltest
from arolla import arolla
from arolla.s11n.testing import codec_test_case

# Import protobufs for the codecs in use:
from arolla.serialization_codecs.dict import dict_codec_pb2 as _
from arolla.serialization_codecs.generic import scalar_codec_pb2 as _


class DictCodecTest(codec_test_case.S11nCodecTestCase):

  def testKeyToRowDictQType(self):
    value = arolla.types.make_key_to_row_dict_qtype(arolla.INT32)
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
        }
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ScalarV1Proto.extension" }
        }
        decoding_steps {
          value {
            codec_index: 1
            [arolla.serialization_codecs.ScalarV1Proto.extension] {
              int32_qtype: true
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: 2
            codec_index: 0
            [arolla.serialization_codecs.DictV1Proto.extension] {
              key_to_row_dict_qtype { }
            }
          }
        }
        decoding_steps {
          output_value_index: 3
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testDictQType(self):
    value = arolla.types.make_dict_qtype(arolla.BYTES, arolla.FLOAT32)
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
        }
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ScalarV1Proto.extension" }
        }
        decoding_steps {
          value {
            codec_index: 1
            [arolla.serialization_codecs.ScalarV1Proto.extension] {
              bytes_qtype: true
            }
          }
        }
        decoding_steps {
          value {
            codec_index: 1
            [arolla.serialization_codecs.ScalarV1Proto.extension] {
              float32_qtype: true
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: [2, 3]
            codec_index: 0
            [arolla.serialization_codecs.DictV1Proto.extension] {
              dict_qtype { }
            }
          }
        }
        decoding_steps {
          output_value_index: 4
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testError_MissingExtension(self):
    with self.assertRaisesRegex(ValueError, re.escape('no extension found;')):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
          }
          decoding_steps { value { codec_index: 0 } }
          """)

  def testError_MissingValue(self):
    with self.assertRaisesRegex(ValueError, re.escape('missing value;')):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.DictV1Proto.extension] { }
            }
          }
          """)

  def testError_KeyToRowDictQType_MissingInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected input_value_indices.size=1, got 0;'
            ' value=KEY_TO_ROW_DICT_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.DictV1Proto.extension] {
                key_to_row_dict_qtype: { }
              }
            }
          }
          """)

  def testError_KeyToRowDictQType_WrongInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a qtype, got input_values[0].qtype=INT32;'
            ' value=KEY_TO_ROW_DICT_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
          }
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_value: 0
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: 2
              [arolla.serialization_codecs.DictV1Proto.extension] {
                key_to_row_dict_qtype: { }
              }
            }
          }
          """)

  def testError_KeyToRowDictQType_UnsupportedKeyQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'no dict with FLOAT32 keys found; value=KEY_TO_ROW_DICT_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
          }
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                float32_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: 2
              [arolla.serialization_codecs.DictV1Proto.extension] {
                key_to_row_dict_qtype: { }
              }
            }
          }
          """)

  def testError_DictQType_MissingInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected input_value_indices.size=2, got 0; value=DICT_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.DictV1Proto.extension] {
                dict_qtype: { }
              }
            }
          }
          """)

  def testError_DictQType_WrongInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a qtype, got input_values[1].qtype=FLOAT32;'
            ' value=DICT_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
          }
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                float32_value: 0.0
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2, 3]
              [arolla.serialization_codecs.DictV1Proto.extension] {
                dict_qtype: { }
              }
            }
          }
          """)

  def testError_DictQType_UnsupportedKeyValueQTypes(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'DenseArray type with elements of type NOTHING is not registered.;'
            ' value=DICT_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.DictV1Proto.extension" }
          }
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                qtype_qtype: false
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2, 3]
              [arolla.serialization_codecs.DictV1Proto.extension] {
                dict_qtype: { }
              }
            }
          }
          """)


if __name__ == '__main__':
  absltest.main()
