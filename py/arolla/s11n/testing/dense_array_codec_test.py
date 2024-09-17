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
from arolla.serialization_codecs.dense_array import dense_array_codec_pb2 as _

_COMMON_HEADER = """
    version: 2
    decoding_steps {
      codec {
        name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
      }
    }
    decoding_steps {
      value {
        codec_index: 0
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
"""

_COMMON_FOOTER = """
        }
      }
    }
    decoding_steps {
      output_value_index: 1
    }
"""


def _gen_text_proto(body_text_proto):
  return _COMMON_HEADER + body_text_proto + _COMMON_FOOTER


_NAN = float('nan')
_INF = float('inf')


class DenseArrayCodecTest(codec_test_case.S11nCodecTestCase):

  @parameterized.named_parameters(
      # Values
      (
          'DenseArrayUnitValue',
          arolla.dense_array_unit([None, True, None]),
          _gen_text_proto("""
           dense_array_unit_value {
             size: 3 bitmap: [2]
           }"""),
      ),
      (
          'DenseArrayBooleanValue',
          arolla.dense_array_boolean([None, False, True, None]),
          _gen_text_proto("""
          dense_array_boolean_value {
            size: 4  bitmap: [6]  values: [false, true]
          }"""),
      ),
      (
          'DenseArrayBytesValue',
          arolla.dense_array_bytes([None, b'', b'foo', b'bar', None]),
          _gen_text_proto("""
           dense_array_bytes_value {
             size: 5  bitmap: [14]  characters: 'foobar'
             value_offset_starts: [0, 0, 3]
             value_offset_ends: [0, 3, 6]
           }"""),
      ),
      (
          'DenseArrayTextValue',
          arolla.dense_array_text([None, '', 'foo', 'bar', None]),
          _gen_text_proto("""
          dense_array_text_value {
            size: 5  bitmap: [14]  characters: 'foobar'
            value_offset_starts: [0, 0, 3]
            value_offset_ends: [0, 3, 6]
          }"""),
      ),
      (
          'DenseArrayInt32Value',
          arolla.dense_array_int32([None, -1, 0, 2147483647, None]),
          _gen_text_proto("""
          dense_array_int32_value {
            size: 5  bitmap: [14]  values: [-1, 0, 2147483647]
          }"""),
      ),
      (
          'DenseArrayInt64Value',
          arolla.dense_array_int64([None, -1, 0, 9223372036854775807, None]),
          _gen_text_proto("""
           dense_array_int64_value {
             size: 5  bitmap: [14]  values: [-1, 0, 9223372036854775807]
          }"""),
      ),
      (
          'DenseArrayUInt64Value',
          arolla.types.dense_array_uint64(
              [None, 0, 18446744073709551615, None]
          ),
          _gen_text_proto("""
          dense_array_uint64_value {
            size: 4  bitmap: [6]  values: [0, 18446744073709551615]
          }"""),
      ),
      (
          'DenseArrayFloat32Value',
          arolla.dense_array_float32([None, _NAN, 1.5, -_INF, None]),
          _gen_text_proto("""
          dense_array_float32_value {
            size: 5  bitmap: [14]  values: [nan, 1.5, -inf]
          }"""),
      ),
      (
          'DenseArrayFloat64Value',
          arolla.dense_array_float64([None, _NAN, 1.5, -_INF, None]),
          _gen_text_proto("""
          dense_array_float64_value {
            size: 5  bitmap: [14]  values: [nan, 1.5, -inf]
          }"""),
      ),
      (
          'DenseArrayToScalarEdgeValue',
          arolla.types.DenseArrayToScalarEdge(4),
          _gen_text_proto(' dense_array_to_scalar_edge_value: 4 '),
      ),
      (
          'DenseArrayShapeValue',
          arolla.types.DenseArrayShape(5),
          _gen_text_proto(' dense_array_shape_value: 5 '),
      ),
      # Special cases
      (
          'Full',
          arolla.dense_array_unit([True for i in range(1024)]),
          _gen_text_proto("""
          dense_array_unit_value {
            size: 1024
          }"""),
      ),
      (
          'AllMissing',
          arolla.dense_array_bytes([None for i in range(1024)]),
          _gen_text_proto("""
          dense_array_bytes_value {
            size: 1024
            bitmap: [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,
                     0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
            characters: ""
          }"""),
      ),
  )
  def testValue(self, qvalue, text):
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    arolla.testing.assert_qvalue_allequal(actual_qvalue, qvalue)

  def testValue_StringsWithOverlaps(self):
    actual_qvalue = self.parse_container_text_proto(_gen_text_proto("""
        dense_array_text_value {
          size: 12
          characters: 'abracadabra'
          value_offset_starts: [11, 10, 7, 0, 3, 5, 8, 1, 4, 6, 9, 2]
          value_offset_ends: [11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11, 11]
        }"""))
    self.assertListEqual(
        actual_qvalue.py_value(),  # pytype: disable=attribute-error
        [
            '',
            'a',
            'abra',
            'abracadabra',
            'acadabra',
            'adabra',
            'bra',
            'bracadabra',
            'cadabra',
            'dabra',
            'ra',
            'racadabra',
        ],
    )

  @parameterized.named_parameters(
      (
          'DenseArrayUnitQType',
          arolla.DENSE_ARRAY_UNIT,
          _gen_text_proto('dense_array_unit_qtype: true'),
      ),
      (
          'DenseArrayBooleanQType',
          arolla.DENSE_ARRAY_BOOLEAN,
          _gen_text_proto('dense_array_boolean_qtype: true'),
      ),
      (
          'DenseArrayBytesQType',
          arolla.DENSE_ARRAY_BYTES,
          _gen_text_proto('dense_array_bytes_qtype: true'),
      ),
      (
          'DenseArrayTextQType',
          arolla.DENSE_ARRAY_TEXT,
          _gen_text_proto('dense_array_text_qtype: true'),
      ),
      (
          'DenseArrayInt32QType',
          arolla.DENSE_ARRAY_INT32,
          _gen_text_proto('dense_array_int32_qtype: true'),
      ),
      (
          'DenseArrayInt64QType',
          arolla.DENSE_ARRAY_INT64,
          _gen_text_proto('dense_array_int64_qtype: true'),
      ),
      (
          'DenseArrayUInt64QType',
          arolla.types.DENSE_ARRAY_UINT64,
          _gen_text_proto('dense_array_uint64_qtype: true'),
      ),
      (
          'DenseArrayFloat32QType',
          arolla.DENSE_ARRAY_FLOAT32,
          _gen_text_proto('dense_array_float32_qtype: true'),
      ),
      (
          'DenseArrayFloat64QType',
          arolla.DENSE_ARRAY_FLOAT64,
          _gen_text_proto('dense_array_float64_qtype: true'),
      ),
      (
          'DenseArrayEdgeQType',
          arolla.DENSE_ARRAY_EDGE,
          _gen_text_proto('dense_array_edge_qtype: true'),
      ),
      (
          'DenseArrayToScalarEdgeQType',
          arolla.DENSE_ARRAY_TO_SCALAR_EDGE,
          _gen_text_proto('dense_array_to_scalar_edge_qtype: true'),
      ),
      (
          'DenseArrayShapeQType',
          arolla.DENSE_ARRAY_SHAPE,
          _gen_text_proto('dense_array_shape_qtype: true'),
      ),
  )
  def testQType(self, qvalue, text):
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def testError_MissingExtension(self):
    with self.assertRaisesRegex(ValueError, re.escape('no extension found;')):
      self.parse_container_text_proto("""
          version: 2
          codecs {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
          decoding_steps {
            value {
              codec_index: 0
            }
          }
          """)

  def testError_MissingValue(self):
    with self.assertRaisesRegex(ValueError, re.escape('missing value;')):
      self.parse_container_text_proto(_gen_text_proto(''))

  # NOTE: There are three groups of implementations:
  #   [unit], [bytes, text], [boolean, int*, float*]
  # Test one implementation from each group.

  @parameterized.parameters(
      'dense_array_unit_value',
      'dense_array_bytes_value',
      'dense_array_int32_value',
  )
  def testError_MissingSize(self, field_name):
    with self.assertRaisesRegex(
        ValueError, re.escape(f'missing field {field_name}.size;')
    ):
      self.parse_container_text_proto(
          _gen_text_proto(field_name + '{ bitmap: [0] }')
      )

  @parameterized.parameters(
      'dense_array_unit_value',
      'dense_array_bytes_value',
      'dense_array_int32_value',
  )
  def testError_NegativeSize(self, field_name):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            f'expected a non-negative value in {field_name}.size, got -1;'
        ),
    ):
      self.parse_container_text_proto(
          _gen_text_proto(field_name + '{ size: -1 }')
      )

  @parameterized.parameters(
      'dense_array_unit_value',
      'dense_array_bytes_value',
      'dense_array_int32_value',
  )
  def testError_IllegalBitmapSize(self, field_name):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(f'expected 2 items in {field_name}.bitmap, got 1;'),
    ):
      self.parse_container_text_proto(
          _gen_text_proto(field_name + '{ size:  33 bitmap: [0xffffffff] }')
      )

  def testError_IllegalValueCount(self):
    # NOTE: This test is for [boolean, int*, float*] implementation group.
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected 4 items in dense_array_int32_value.values, got 3;'),
    ):
      self.parse_container_text_proto(_gen_text_proto("""
              dense_array_int32_value {
                size: 5  bitmap: [0xf]  values: [1, 2, 3]
              }"""))

  def testError_MissingCharacters(self):
    # NOTE: This test is for [bytes, text] implementation group.
    with self.assertRaisesRegex(
        ValueError,
        re.escape('missing field dense_array_bytes_value.characters;'),
    ):
      self.parse_container_text_proto(_gen_text_proto("""
              dense_array_bytes_value {
                size: 5  bitmap: [0xf]
                value_offset_starts: [0, 0, 0, 0]
                value_offset_ends: [1, 1, 1, 1]
              }"""))

  def testError_IllegalValueOffsetCount(self):
    # NOTE: This test is for [bytes, text] implementation group.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected 4 items in dense_array_bytes_value.value_offset_starts,'
            ' got 3;'
        ),
    ):
      self.parse_container_text_proto(_gen_text_proto("""
              dense_array_bytes_value {
                size: 5  bitmap: [0xf]  characters: 'abcde'
                value_offset_starts: [1, 2, 3]
                value_offset_ends: [1, 2, 3, 4]
              }"""))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected 4 items in dense_array_bytes_value.value_offset_ends,'
            ' got 3;'
        ),
    ):
      self.parse_container_text_proto(_gen_text_proto("""
              dense_array_bytes_value {
                size: 5  bitmap: [0xf]  characters: 'abcde'
                value_offset_starts: [1, 2, 3, 4]
                value_offset_ends: [1, 2, 3]
              }"""))

  def testError_IllegalValueOffset(self):
    # NOTE: This test is for [bytes, text] implementation group.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected non-negative items in'
            ' dense_array_bytes_value.value_offset_starts, got -3;'
        ),
    ):
      self.parse_container_text_proto(_gen_text_proto("""
              dense_array_bytes_value {
                size: 5  bitmap: [0xf]
                characters: 'a'
                value_offset_starts: [0, 0, 0, -3]
                value_offset_ends: [1, 1, 1, 1]
              }"""))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected items in dense_array_bytes_value.value_offset_starts to'
            ' be less-or-equal than corresponding .value_offset_ends, got 1'
            ' greater than 0;'
        ),
    ):
      self.parse_container_text_proto(_gen_text_proto("""
              dense_array_bytes_value {
                size: 5  bitmap: [0xf]
                characters: 'a'
                value_offset_starts: [0, 0, 0, 1]
                value_offset_ends: [1, 1, 1, 0]
              }"""))
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected items in dense_array_bytes_value.value_offset_ends to be'
            ' less-or-equal than .characters size, got 6 greater than 5;'
        ),
    ):
      self.parse_container_text_proto(_gen_text_proto("""
              dense_array_bytes_value {
                size: 5  bitmap: [0xf]
                characters: 'abcde'
                value_offset_starts: [0, 0, 0, 0]
                value_offset_ends: [1, 1, 1, 6]
              }"""))

  def testError_NegativeSizeShape(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected non-negative dense_array_shape_value, got -4;'),
    ):
      self.parse_container_text_proto(
          _gen_text_proto('dense_array_shape_value: -4')
      )

  def testError_NegativeSizeToScalarEdge(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected non-negative dense_array_to_scalar_edge_value, got -3;'
        ),
    ):
      self.parse_container_text_proto(
          _gen_text_proto('dense_array_to_scalar_edge_value: -3')
      )

  def testValue_EdgeMapping(self):
    qvalue = arolla.types.DenseArrayEdge.from_mapping([1, 2, 3], 5)
    text = """
        version: 2
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_int64_value {
                size: 3
                values: [1, 2, 3]
              }
            }
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            input_value_indices: 1
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_edge_value { edge_type: MAPPING parent_size: 5 }
            }
          }
        }
        decoding_steps {
          output_value_index: 2
        }
        """
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    self.assertEqual(actual_qvalue.fingerprint, qvalue.fingerprint)

  def testValue_EdgeSplitPoints(self):
    qvalue = arolla.types.DenseArrayEdge.from_split_points([0, 1, 2, 3])
    text = """
        version: 2
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_int64_value {
                size: 4
                values: [0, 1, 2, 3]
              }
            }
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            input_value_indices: 1
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_edge_value { edge_type: SPLIT_POINTS }
            }
          }
        }
        decoding_steps {
          output_value_index: 2
        }
        """
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    self.assertEqual(actual_qvalue.fingerprint, qvalue.fingerprint)

  def _gen_edge_error_proto(self, value_proto):
    return """
        version: 2
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_int32_value { size: 0 }
            }
          }
        }
        decoding_steps {
          value {
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_int64_value { size: 0 }
            }
          }
        }
        decoding_steps {
          value { %s }
        }
        """ % value_proto

  def testError_EdgeWrongInputValueCount(self):
    text = self._gen_edge_error_proto("""
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_edge_value { edge_type: SPLIT_POINTS }
        }
        """)
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 1 item in input_values, got 0;')
    ):
      self.parse_container_text_proto(text)

  def testError_EdgeWrongInputValueType(self):
    text = self._gen_edge_error_proto("""
        input_value_indices: 1
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_edge_value { edge_type: SPLIT_POINTS }
        }
        """)
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected DENSE_ARRAY_INT64 in input_values[0], got'
            ' DENSE_ARRAY_INT32;'
        ),
    ):
      self.parse_container_text_proto(text)

  def testError_MissingEdgeType(self):
    text = self._gen_edge_error_proto("""
        input_value_indices: 2
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_edge_value { }
        }
        """)
    with self.assertRaisesRegex(
        ValueError, re.escape('missing field dense_array_edge_value.edge_type;')
    ):
      self.parse_container_text_proto(text)

  def testError_EdgeUnknownEdgeType(self):
    text = self._gen_edge_error_proto("""
        input_value_indices: 2
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_edge_value { edge_type: EDGE_TYPE_UNSPECIFIED }
        }
        """)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('unsupported value in dense_array_edge_value.edge_type: 0;'),
    ):
      self.parse_container_text_proto(text)

  def testError_EdgeUnexpectedGroupSize(self):
    text = self._gen_edge_error_proto("""
        input_value_indices: 2
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_edge_value { edge_type: SPLIT_POINTS  parent_size: 0 }
        }
        """)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('unexpected field dense_array_edge_value.parent_size;'),
    ):
      self.parse_container_text_proto(text)

  def testError_EdgeMissingGroupSize(self):
    text = self._gen_edge_error_proto("""
        input_value_indices: 2
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_edge_value { edge_type: MAPPING }
        }
        """)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('missing field dense_array_edge_value.parent_size;'),
    ):
      self.parse_container_text_proto(text)

  def testError_EdgeNegativeGroupSize(self):
    text = self._gen_edge_error_proto("""
        input_value_indices: 2
        [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
          dense_array_edge_value { edge_type: MAPPING  parent_size: -1 }
        }
        """)
    with self.assertRaisesRegex(
        ValueError, re.escape('parent_size can not be negative;')
    ):
      self.parse_container_text_proto(text)


if __name__ == '__main__':
  absltest.main()
