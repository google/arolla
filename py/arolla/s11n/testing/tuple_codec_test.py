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
from absl.testing import parameterized
from arolla import arolla
from arolla.s11n.testing import codec_test_case

# Import protobufs for the codecs in use:
from arolla.serialization_codecs.generic import tuple_codec_pb2 as _


class TupleCodecTest(codec_test_case.S11nCodecTestCase):

  @parameterized.named_parameters(
      (
          'EmptyTupleValue',
          arolla.tuple(),
          """
          version: 2
          decoding_steps {  # [0]
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [2]
            output_value_index: 1
          }
          """,
      ),
      (
          'TupleValue',
          arolla.tuple(arolla.tuple(), arolla.tuple(arolla.tuple())),
          """
          version: 2
          decoding_steps {  # [0]
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 0
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [3]
            value {
              codec_index: 0
              input_value_indices: [1, 2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [4]
            output_value_index: 3
          }
          """,
      ),
      (
          'EmptyTupleQType',
          arolla.tuple().qtype,
          """
          version: 2
          decoding_steps {  # [0]
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [2]
            output_value_index: 1
          }
          """,
      ),
      (
          'TupleQType',
          arolla.tuple(arolla.tuple(), arolla.tuple(arolla.tuple())).qtype,
          """
          version: 2
          decoding_steps {  # [0]
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 0
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [3]
            value {
              codec_index: 0
              input_value_indices: [1, 2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [4]
            output_value_index: 3
          }
          """,
      ),
      (
          'EmptyNamedTupleValue',
          arolla.namedtuple(),
          """
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.TupleV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_value: { }
              }
            }
          }
          decoding_steps {  # [2]
            output_value_index: 1
          }""",
      ),
      (
          'EmptyNamedTupleQType',
          arolla.namedtuple().qtype,
          """
          version: 2
          decoding_steps {  # [0]
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 0
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_qtype: { }
              }
            }
          }
          decoding_steps {  # [3]
            output_value_index: 2
          }
          """,
      ),
      (
          'NamedTupleValue',
          arolla.namedtuple(x=arolla.tuple(), y=arolla.tuple(arolla.tuple())),
          """
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.TupleV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 0
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [3]
            value {
              codec_index: 0
              input_value_indices: [1, 2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_value { field_names: ['x', 'y'] }
              }
            }
          }
          decoding_steps {
            output_value_index: 3
          }""",
      ),
      (
          'NamedTupleQType',
          arolla.namedtuple(
              x=arolla.tuple(), y=arolla.tuple(arolla.tuple())
          ).qtype,
          """
          version: 2
          decoding_steps {  # [0]
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 0
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [3]
            value {
              codec_index: 0
              input_value_indices: [1, 2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [4]
            value {
              codec_index: 0
              input_value_indices: [3]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_qtype { field_names: ['x', 'y'] }
              }
            }
          }
          decoding_steps {  # [5]
            output_value_index: 4
          }
          """,
      ),
      (
          'SliceValue',
          arolla.types.Slice(
              start=arolla.tuple(), stop=arolla.tuple(), step=arolla.tuple()
          ),
          """
          version: 2
          decoding_steps {  # [0]
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 0
              input_value_indices: [1, 1, 1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [3]
            value {
              codec_index: 0
              input_value_indices: [2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                slice_value: true
              }
            }
          }
          decoding_steps {  # [4]
            output_value_index: 3
          }
          """,
      ),
      (
          'SliceQType',
          arolla.types.Slice(
              start=arolla.tuple(), stop=arolla.tuple(), step=arolla.tuple()
          ).qtype,
          """
          version: 2
          decoding_steps {  # [0]
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 0
              input_value_indices: [1, 1, 1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {  # [3]
            value {
              codec_index: 0
              input_value_indices: [2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                slice_qtype: true
              }
            }
          }
          decoding_steps {  # [4]
            output_value_index: 3
          }
          """,
      ),
  )
  def test(self, qvalue, text):
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def testError_MissingExtension(self):
    with self.assertRaisesRegex(ValueError, re.escape('no extension found;')):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value { codec_index: 0 }
          }
          """)

  def testError_MissingValue(self):
    with self.assertRaisesRegex(ValueError, re.escape('missing value;')):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] { }
            }
          }
          """)

  def testError_TupleQTypeInvalidInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a qtype, got a tuple<> value as an input;'
            ' value=TUPLE_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          """)

  def testError_NamedTupleQTypeMissingInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a single input value, got 0; value=NAMEDTUPLE_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_qtype {}
              }
            }
          }
          """)

  def testError_NamedTupleQTypeInputIsNotQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a qtype, got a tuple<> value as an input;'
            ' value=NAMEDTUPLE_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_qtype { field_names: ['x'] }
              }
            }
          }
          """)

  def testError_NamedTupleQTypeInputIsNotTupleQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a tuple qtype, got namedtuple<> as an input;'
            ' value=NAMEDTUPLE_QTYPE;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_qtype { }
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_qtype { }
              }
            }
          }
          """)

  def testError_NamedTupleFieldValueAndFieldNamesDiscrepancy(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'incorrect NamedTupleQType #field_names != #fields: 2 vs 1;'
            ' value=NAMEDTUPLE;'
        ),
    ):
      self.parse_container_text_proto(
          """
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.TupleV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              codec_index: 0
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {  # [2]
            value {
              codec_index: 0
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                namedtuple_value { field_names: ['x', 'y'] }
              }
            }
          }
          decoding_steps {
            output_value_index: 3
          }""",
      )

  def testError_SliceQTypeInvalidInputValue(self):
    with self.assertRaisesRegex(
        ValueError, 'expected.*Tuple.*got.*QType.*; value=SLICE_QTYPE;'
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [1]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                slice_qtype: true
              }
            }
          }
              """)

  def testError_SliceQTypeTooFewValues(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a single input value, got 0; value=SLICE_QTYPE;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                slice_qtype: true
              }
            }
          }
          """)

  def testError_SliceQTypeTooFewFields(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected 3 qtypes (start, stop, step), got 2; value=SLICE_QTYPE;'
        ),
    ):
      self.parse_container_text_proto(
          """
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [1, 1]  # Only 2 fields
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                slice_qtype: true
              }
            }
          }
          """,
      )

  def testError_SliceValueTooFewValues(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected a single input value, got 0; value=SLICE;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                slice_value: true
              }
            }
          }
          """)

  def testError_SliceValueTooFewFields(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a 3-tuple (start, stop, step), got ((), ()); value=SLICE;'
        ),
    ):
      self.parse_container_text_proto(
          """
          version: 2
          decoding_steps {
            codec { name: "arolla.serialization_codecs.TupleV1Proto.extension" }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [1, 1]  # Only 2 fields
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                tuple_value: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [2]
              [arolla.serialization_codecs.TupleV1Proto.extension] {
                slice_value: true
              }
            }
          }
          """,
      )


if __name__ == '__main__':
  absltest.main()
