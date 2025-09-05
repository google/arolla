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
from arolla import arolla
from arolla.s11n.testing import codec_test_case

# Import protobufs for the codecs in use:
from arolla.sequence.s11n import codec_pb2 as _
from arolla.serialization_codecs.generic import scalar_codec_pb2 as _


class S11nTest(codec_test_case.S11nCodecTestCase):

  def test_qtype(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
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
            int32_qtype: true
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          codec_index: 0
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 3
      }
    """
    qtype = arolla.types.make_sequence_qtype(arolla.types.INT32)
    self.assertDumpsEqual(qtype, text)
    self.assertLoadsEqual(text, qtype)

  def test_empty_sequence(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
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
            qtype_qtype: false
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          codec_index: 0
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 3
      }
    """
    qvalue = arolla.types.Sequence()
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_int_sequence(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
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
            int32_qtype: true
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
          codec_index: 1
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int32_value: 2
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          input_value_indices: 3
          input_value_indices: 4
          codec_index: 0
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 5
      }
    """
    qvalue = arolla.types.Sequence(1, 2)
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_too_few_input_values_qtype_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_qtype: true
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected input_value_indices.size=1, got 0; value=SEQUENCE_QTYPE'
        ),
    ):
      self.parse_container_text_proto(text)

  def test_too_many_input_values_qtype_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
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
            int32_qtype: true
          }
        }
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
          input_value_indices: 3
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_qtype: true
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected input_value_indices.size=1, got 2; value=SEQUENCE_QTYPE'
        ),
    ):
      self.parse_container_text_proto(text)

  def test_value_qtype_not_a_qtype(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
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
            int32_value: 2
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_qtype: true
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a qtype, got input_values[0].qtype=INT32;'
            ' value=SEQUENCE_QTYPE'
        ),
    ):
      self.parse_container_text_proto(text)

  def test_too_few_input_values_qvalue_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_value: true
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected non-empty input_values; value=SEQUENCE_VALUE'),
    ):
      self.parse_container_text_proto(text)

  def test_first_value_not_a_qtype_in_qvalue_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
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
            int32_value: 1
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_value: true
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a qtype, got input_values[0].qtype=INT32;'
            ' value=SEQUENCE_VALUE'
        ),
    ):
      self.parse_container_text_proto(text)

  def test_data_qtype_mismatch_in_qvalue_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
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
            int32_qtype: true
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
          codec_index: 1
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            float32_value: 2.0
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          input_value_indices: 3
          input_value_indices: 4
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
            sequence_value: true
          }
        }
      }
    """
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected INT32, got input_values[2].qtype=FLOAT32;'
            ' value=SEQUENCE_VALUE'
        ),
    ):
      self.parse_container_text_proto(text)

  def test_missing_value_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.SequenceV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.SequenceV1Proto.extension] {
          }
        }
      }
    """
    with self.assertRaisesRegex(ValueError, 'missing value'):
      self.parse_container_text_proto(text)


if __name__ == '__main__':
  absltest.main()
