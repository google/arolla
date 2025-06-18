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
from absl.testing import parameterized
from arolla import arolla
from arolla.derived_qtype import derived_qtype
from arolla.s11n.testing import codec_test_case

# Import protobufs for the codecs in use:
from arolla.derived_qtype.s11n import codec_pb2 as _
from arolla.serialization_codecs.generic import scalar_codec_pb2 as _

M = arolla.M | derived_qtype.M
L = arolla.L


def get_labeled_qtype(qtype: arolla.QType, label: str) -> arolla.QType:
  return arolla.eval(M.derived_qtype.get_labeled_qtype(qtype, label))


def get_labeled_qvalue(value: arolla.QValue, label: str) -> arolla.QValue:
  return arolla.eval(
      M.derived_qtype.downcast(
          M.derived_qtype.get_labeled_qtype(value.qtype, label), value
      )
  )


class DerivedQTypeS11NTest(codec_test_case.S11nCodecTestCase):

  @parameterized.named_parameters(
      (
          'LabeledQType',
          get_labeled_qtype(arolla.INT32, 'foo'),
          """
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 1
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_qtype: true
              }
            }
          }
          decoding_steps {  # [3]
            value {
              codec_index: 0
              input_value_indices: [2]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                labeled_qtype { label: "foo" }
              }
            }
          }
          decoding_steps {  # [4]
            output_value_index: 3
          }
          """,
      ),
      (
          'LabeledDerivedValue',
          get_labeled_qvalue(arolla.int32(1), 'foo'),
          """
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 1
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_value: 1
              }
            }
          }
          decoding_steps {  # [3]
            value {
              codec_index: 1
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_qtype: true
              }
            }
          }
          decoding_steps {  # [4]
            value {
              codec_index: 0
              input_value_indices: [3]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                labeled_qtype { label: "foo" }
              }
            }
          }
          decoding_steps {  # [5]
            value {
              codec_index: 0
              input_value_indices: [2, 4]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                derived_value { }
              }
            }
          }
          decoding_steps {  # [6]
            output_value_index: 5
          }
          """,
      ),
  )
  def test(self, qvalue, text):
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def testError_MissingValue(self):
    with self.assertRaisesRegex(ValueError, re.escape('missing value;')):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] { }
            }
          }
          """)

  def testError_LabeledQTypeMissingInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a single input value, got 0; value=LABELED_QTYPE'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                labeled_qtype { label: "foo" }
              }
            }
          }
          """)

  def testError_LabeledQTypeInputIsNotQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a qtype in input_values[0], got a INT32 value;'
            ' value=LABELED_QTYPE'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
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
                int32_value: 1
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                labeled_qtype { label: "foo" }
              }
            }
          }
          """)

  def testError_LabeledQTypeMissingLabel(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('missing label; value=LABELED_QTYPE')
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
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
                int32_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                labeled_qtype { }
              }
            }
          }
          """)

  def testError_DerivedValueMissingInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected two input values, got 0; value=DERIVED_VALUE'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                derived_value { }
              }
            }
          }
          """)

  def testError_DerivedValueSecondInputIsNotQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a derived qtype in input_values[1], got a INT32 value;'
            ' value=DERIVED_VALUE'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
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
                int32_value: 1
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2, 2]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                derived_value { }
              }
            }
          }
          """)

  def testError_DerivedValueSecondInputIsNotDerivedQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a derived qtype in input_values[1], got INT32;'
            ' value=DERIVED_VALUE'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
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
                int32_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2, 2]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                derived_value { }
              }
            }
          }
          """)

  def testError_DerivedValueBaseTypeDoesNotMatch(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a value of type INT32 in input_values[0], got'
            ' FLOAT32; value=DERIVED_VALUE'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.DerivedQTypeV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {  # [2]
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                float32_value: 0.5
              }
            }
          }
          decoding_steps {  # [3]
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_qtype: true
              }
            }
          }
          decoding_steps {  # [4]
            value {
              input_value_indices: [3]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                labeled_qtype { label: "foo" }
              }
            }
          }
          decoding_steps {  # [5]
            value {
              input_value_indices: [2, 4]
              [arolla.serialization_codecs.DerivedQTypeV1Proto.extension] {
                derived_value { }
              }
            }
          }
          """)


if __name__ == '__main__':
  absltest.main()
