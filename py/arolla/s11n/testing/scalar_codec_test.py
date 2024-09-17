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
from arolla.serialization_codecs.generic import scalar_codec_pb2 as _

UINT64 = arolla.types.UINT64
uint64 = arolla.types.uint64


class ScalarCodecTest(codec_test_case.S11nCodecTestCase):

  @parameterized.named_parameters(
      (
          'UnitValue',
          arolla.unit(),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                unit_value: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'BooleanValue',
          arolla.boolean(False),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                boolean_value: false
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'BytesValue',
          arolla.bytes(b'foo'),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                bytes_value: "foo"
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'TextValue',
          arolla.text('bar'),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                text_value: "bar"
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'Int32Value',
          arolla.int32(1234567890),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_value: 1234567890
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'Int64Value',
          arolla.int64(1234567890123456789),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int64_value: 1234567890123456789
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'UInt64Value',
          uint64(12345678901234567890),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                uint64_value: 12345678901234567890
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'Float32Value',
          arolla.float32(0.5),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                float32_value: 0.5
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'Float64Value',
          arolla.float64(0.5),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                float64_value: 0.5
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'WeakFloatValue',
          arolla.weak_float(0.5),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                weak_float_value: 0.5
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'ScalarToScalarEdgeValue',
          arolla.types.ScalarToScalarEdge(),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                scalar_to_scalar_edge_value: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'ScalarShapeValue',
          arolla.types.ScalarShape(),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                scalar_shape_value: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'UnspecifiedValue',
          arolla.unspecified(),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                unspecified_value: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'ExprQuote',
          arolla.abc.quote(arolla.L.x),
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            leaf_node {
              leaf_key: "x"
            }
          }
          decoding_steps {
            value {
              input_expr_indices: 1
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                expr_quote_value: true
              }
            }
          }
          decoding_steps {
            output_value_index: 2
          }
          """,
      ),
      (
          'UnitQType',
          arolla.UNIT,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                unit_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'BooleanQType',
          arolla.BOOLEAN,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                boolean_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'BytesQType',
          arolla.BYTES,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                bytes_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'TextQType',
          arolla.TEXT,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                text_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'Int32QType',
          arolla.INT32,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int32_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'Int64QType',
          arolla.INT64,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                int64_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'UInt64QType',
          UINT64,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                uint64_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'Float32QType',
          arolla.FLOAT32,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                float32_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'Float64QType',
          arolla.FLOAT64,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                float64_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'WeakFloatQType',
          arolla.WEAK_FLOAT,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                weak_float_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'ScalarToScalarEdge',
          arolla.types.SCALAR_TO_SCALAR_EDGE,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                scalar_to_scalar_edge_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'ScalarShapeQType',
          arolla.SCALAR_SHAPE,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                scalar_shape_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'UnspecifiedQType',
          arolla.UNSPECIFIED,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                unspecified_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'QTypeQType',
          arolla.QTYPE,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                qtype_qtype: true
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'NothingQType',
          arolla.NOTHING,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                qtype_qtype: false
              }
            }
          }
          decoding_steps {
            output_value_index: 1
          }
          """,
      ),
      (
          'ExprQuoteQType',
          arolla.abc.EXPR_QUOTE,
          """
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                expr_quote_qtype: true
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
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps { value { codec_index: 0 } }
          """)

  def testError_MissingValue(self):
    with self.assertRaisesRegex(ValueError, re.escape('missing value;')):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value { [arolla.serialization_codecs.ScalarV1Proto.extension] {} }
          }
          """)

  def testError_ExprQuoteMissingExpr(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('ExprQuote requires exactly one input expr, got 0;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                 expr_quote_value: true
              }
            }
          }
          """)


if __name__ == '__main__':
  absltest.main()
