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
from arolla.serialization_codecs.generic import optional_codec_pb2 as _

OPTIONAL_UINT64 = arolla.types.OPTIONAL_UINT64
optional_uint64 = arolla.types.optional_uint64


class OptionalCodecTest(codec_test_case.S11nCodecTestCase):

  @parameterized.named_parameters(
      (
          'MissingUnitValue',
          arolla.missing_unit(),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_unit_value: false
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentUnitValue',
          arolla.present_unit(),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_unit_value: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingBooleanValue',
          arolla.optional_boolean(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_boolean_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentBooleanValue',
          arolla.optional_boolean(False),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_boolean_value { value: false }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingBytesValue',
          arolla.optional_bytes(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_bytes_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentBytesValue',
          arolla.optional_bytes(b'FOO'),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_bytes_value { value: "FOO" }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingTextValue',
          arolla.optional_text(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_text_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentTextValue',
          arolla.optional_text('BAR'),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_text_value { value: "BAR" }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingInt32Value',
          arolla.optional_int32(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_int32_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentInt32Value',
          arolla.optional_int32(1234567890),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_int32_value { value: 1234567890 }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingInt64Value',
          arolla.optional_int64(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_int64_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentInt64Value',
          arolla.optional_int64(1234567890123456789),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_int64_value { value: 1234567890123456789 }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingUInt64Value',
          optional_uint64(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_uint64_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentUInt64Value',
          optional_uint64(12345678901234567890),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_uint64_value { value: 12345678901234567890 }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingFloat32Value',
          arolla.optional_float32(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_float32_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentFloat32Value',
          arolla.optional_float32(0.5),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_float32_value { value: 0.5 }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingFloat64Value',
          arolla.optional_float64(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_float64_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentFloat64Value',
          arolla.optional_float64(0.5),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_float64_value { value: 0.5 }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'MissingWeakFloatValue',
          arolla.optional_weak_float(None),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_weak_float_value {}
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'PresentWeakFloatValue',
          arolla.optional_weak_float(0.5),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_weak_float_value { value: 0.5 }
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalShapeValue',
          arolla.types.OptionalScalarShape(),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_shape_value: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalUnitQType',
          arolla.OPTIONAL_UNIT,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_unit_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalBooleanQType',
          arolla.OPTIONAL_BOOLEAN,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_boolean_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalBytesQType',
          arolla.OPTIONAL_BYTES,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_bytes_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionlTextQType',
          arolla.OPTIONAL_TEXT,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_text_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalInt32QType',
          arolla.OPTIONAL_INT32,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_int32_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalInt64QType',
          arolla.OPTIONAL_INT64,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_int64_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalUInt64QType',
          OPTIONAL_UINT64,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_uint64_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalFloat32QType',
          arolla.OPTIONAL_FLOAT32,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_float32_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalFloat64QType',
          arolla.OPTIONAL_FLOAT64,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_float64_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'OptionalShapeQType',
          arolla.types.OPTIONAL_SCALAR_SHAPE,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] {
                optional_shape_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
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
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
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
            codec {
              name: "arolla.serialization_codecs.OptionalV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.OptionalV1Proto.extension] { }
            }
          }
          """)


if __name__ == '__main__':
  absltest.main()
