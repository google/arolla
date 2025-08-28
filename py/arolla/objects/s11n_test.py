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

import re

from absl.testing import absltest
from arolla.objects import objects
from arolla.s11n.testing import codec_test_case

# Import protobufs for the codecs in use:
from arolla.objects.s11n import codec_pb2 as _
from arolla.serialization_codecs.generic import scalar_codec_pb2 as _


class S11nTest(codec_test_case.S11nCodecTestCase):

  def test_qtype(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    qtype = objects.OBJECT
    self.assertDumpsEqual(qtype, text)
    self.assertLoadsEqual(text, qtype)

  def test_empty_object(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_value {
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    qvalue = objects.Object()
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_object_with_attrs_no_prototype(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 1
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            float32_value: 2.0
          }
        }
      }
      decoding_steps {
        value {
          codec_index: 1
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int32_value: 1
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          input_value_indices: 3
          codec_index: 0
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_value {
              keys: "a"
              keys: "b"
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 4
      }
    """
    qvalue = objects.Object(b=1, a=2.0)
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_object_with_attrs_and_prototype(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 1
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int32_value: 3
          }
        }
      }
      decoding_steps {
        value {
          codec_index: 1
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            float32_value: 2.0
          }
        }
      }
      decoding_steps {
        value {
          codec_index: 1
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int32_value: 1
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 4
          codec_index: 0
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_value {
              keys: "a"
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          input_value_indices: 3
          input_value_indices: 5
          codec_index: 0
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_value {
              keys: "a"
              keys: "b"
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 6
      }
    """
    obj = objects.Object(a=1)
    qvalue = objects.Object(obj, b=2.0, a=3)
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_too_many_input_values_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            float32_value: 2.0
          }
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int32_value: 1
          }
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int32_value: 3
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          input_value_indices: 3
          input_value_indices: 4
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_value {
              keys: "a"
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 5
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected input_value.size==keys_size() (+1), got'
            ' input_value.size=3, keys_size=1; value=OBJECT'
        ),
    ):
      self.parse_container_text_proto(text)

  def test_too_few_input_values_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_value {
              keys: "a"
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 2
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected input_value.size==keys_size() (+1), got'
            ' input_value.size=0, keys_size=1; value=OBJECT'
        ),
    ):
      self.parse_container_text_proto(text)

  def test_duplicate_keys_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            float32_value: 2.0
          }
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int32_value: 1
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          input_value_indices: 3
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_value {
              keys: "a"
              keys: "a"
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 4
      }
    """
    with self.assertRaisesRegex(
        ValueError, re.escape("duplicate key='a'; value=OBJECT")
    ):
      self.parse_container_text_proto(text)

  def test_not_an_object_prototype_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int32_value: 3
          }
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            float32_value: 2.0
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          input_value_indices: 3
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
            object_value {
              keys: "a"
            }
          }
        }
      }
      decoding_steps {
        output_value_index: 4
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        'expected prototype to be OBJECT, got FLOAT32; value=OBJECT',
    ):
      self.parse_container_text_proto(text)

  def test_missing_value_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ObjectsV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.ObjectsV1Proto.extension] {
          }
        }
      }
    """
    with self.assertRaisesRegex(ValueError, 'missing value'):
      self.parse_container_text_proto(text)


if __name__ == '__main__':
  absltest.main()
