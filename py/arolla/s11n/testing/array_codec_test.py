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
import typing

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.s11n.testing import codec_test_case

# Import protobufs for the codecs in use:
from arolla.serialization_codecs.array import array_codec_pb2 as _
from arolla.serialization_codecs.dense_array import dense_array_codec_pb2 as _
from arolla.serialization_codecs.generic import optional_codec_pb2 as _


class ArrayCodecTest(codec_test_case.S11nCodecTestCase):

  @parameterized.parameters(
      ('unit', arolla.array_unit([])),
      ('boolean', arolla.array_boolean([])),
      ('bytes', arolla.array_bytes([])),
      ('text', arolla.array_text([])),
      ('int32', arolla.array_int32([])),
      ('int64', arolla.array_int64([])),
      ('uint64', arolla.types.array_uint64([])),
      ('float32', arolla.array_float32([])),
      ('float64', arolla.array_float64([])),
  )
  def testValue_EmptyArray(self, type_name, qvalue):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_%s_value {
                size: 0
              }
            }
          }
        }
        decoding_steps {
          output_value_index: 1
        }
        """ % type_name
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    arolla.testing.assert_qvalue_allequal(actual_qvalue, qvalue)

  @parameterized.parameters(
      ('unit', arolla.array_unit([True]), ''),
      ('boolean', arolla.array_boolean([True]), 'values: true'),
      (
          'bytes',
          arolla.array_bytes([b'foo']),
          'characters: "foo" value_offset_starts: 0 value_offset_ends: 3',
      ),
      (
          'text',
          arolla.array_text(['bar']),
          'characters: "bar" value_offset_starts: 0 value_offset_ends: 3',
      ),
      ('int32', arolla.array_int32([1]), 'values: 1'),
      ('int64', arolla.array_int64([1]), 'values: 1'),
      ('uint64', arolla.types.array_uint64([1]), 'values: 1'),
      ('float32', arolla.array_float32([1.5]), 'values: 1.5'),
      ('float64', arolla.array_float64([1.5]), 'values: 1.5'),
  )
  def testValue_DenseFormArray(
      self, type_name, qvalue, dense_array_proto_extras
  ):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 1
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_%s_value {
                size: 1
                %s
              }
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: 2
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_%s_value {
                size: 1
              }
            }
          }
        }
        decoding_steps {
          output_value_index: 3
        }
        """ % (
        type_name,
        dense_array_proto_extras,
        type_name,
    )
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    arolla.testing.assert_qvalue_allequal(actual_qvalue, qvalue)

  @parameterized.parameters(
      (
          'unit',
          (arolla.array_unit(values=[True], ids=[0], size=10) | arolla.unit()),
          '',
          ': true',
      ),
      (
          'boolean',
          (arolla.array_boolean(values=[False], ids=[0], size=10) | True),
          'values: false',
          '{ value: true }',
      ),
      (
          'bytes',
          (arolla.array_bytes(values=[b'foo'], ids=[0], size=10) | b'bar'),
          'characters: "foo" value_offset_starts: 0 value_offset_ends: 3',
          '{ value: "bar" }',
      ),
      (
          'text',
          (arolla.array_text(values=['bar'], ids=[0], size=10) | 'foo'),
          'characters: "bar" value_offset_starts: 0 value_offset_ends: 3',
          '{ value: "foo" }',
      ),
      (
          'int32',
          (arolla.array_int32(values=[1], ids=[0], size=10) | arolla.int32(3)),
          'values: 1',
          '{ value: 3 }',
      ),
      (
          'int64',
          (arolla.array_int64(values=[1], ids=[0], size=10) | arolla.int64(4)),
          'values: 1',
          '{ value: 4 }',
      ),
      (
          'uint64',
          (arolla.types.array_uint64(values=[1], ids=[0], size=10)),
          'values: 1',
          '{ }',
      ),
      (
          'float32',
          (
              arolla.array_float32(values=[1], ids=[0], size=10)
              | arolla.float32(5.5)
          ),
          'values: 1',
          '{ value: 5.5 }',
      ),
      (
          'float64',
          (
              arolla.array_float64(values=[1], ids=[0], size=10)
              | arolla.float64(6.5)
          ),
          'values: 1',
          '{ value: 6.5 }',
      ),
  )
  def testValue_SparseFormArray(
      self, type_name, qvalue, dense_array_proto_extras, optional_proto_extras
  ):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 1
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_%s_value {
                size: 1
                %s
              }
            }
          }
        }
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.OptionalV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 3
            [arolla.serialization_codecs.OptionalV1Proto.extension] {
              optional_%s_value %s
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: [2, 4]
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_%s_value {
                size: 10
                ids: [0]
              }
            }
          }
        }
        decoding_steps {
          output_value_index: 5
        }
        """ % (
        type_name,
        dense_array_proto_extras,
        type_name,
        optional_proto_extras,
        type_name,
    )
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    arolla.testing.assert_qvalue_allequal(actual_qvalue, qvalue)

  @parameterized.parameters(
      (
          'unit',
          (arolla.array_unit([], ids=[], size=5) | arolla.unit()),
          '',
          ': true',
      ),
      (
          'boolean',
          (arolla.array_boolean([], ids=[], size=5) | True),
          '',
          '{ value: true}',
      ),
      (
          'bytes',
          (arolla.array_bytes([], ids=[], size=5) | b'foo'),
          'characters: ""',
          '{ value: "foo" }',
      ),
      (
          'text',
          (arolla.array_text([], ids=[], size=5) | 'bar'),
          'characters: ""',
          '{ value: "bar" }',
      ),
      (
          'int32',
          (arolla.array_int32([], ids=[], size=5) | arolla.int32(3)),
          '',
          '{ value: 3 }',
      ),
      (
          'int64',
          (arolla.array_int64([], ids=[], size=5) | arolla.int64(4)),
          '',
          '{ value: 4 }',
      ),
      ('uint64', arolla.types.array_uint64([], ids=[], size=5), '', '{}'),
      (
          'float32',
          (arolla.array_float32([], ids=[], size=5) | 6.5),
          '',
          '{ value: 6.5 }',
      ),
      (
          'float64',
          (arolla.array_float64([], ids=[], size=5) | 7.5),
          '',
          '{ value: 7.5 }',
      ),
  )
  def testValue_ConstFormArray(
      self, type_name, qvalue, dense_array_proto_extras, optional_proto_extras
  ):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 1
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_%s_value { size: 0 %s }
            }
          }
        }
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.OptionalV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 3
            [arolla.serialization_codecs.OptionalV1Proto.extension] {
              optional_%s_value %s
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: [2, 4]
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_%s_value { size: 5 }
            }
          }
        }
        decoding_steps {
          output_value_index: 5
        }
        """ % (
        type_name,
        dense_array_proto_extras,
        type_name,
        optional_proto_extras,
        type_name,
    )
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    arolla.testing.assert_qvalue_allequal(actual_qvalue, qvalue)

  def testValue_ArrayToScalarEdge(self):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_to_scalar_edge_value: 6
            }
          }
        }
        decoding_steps {
          output_value_index: 1
        }
        """
    qvalue = arolla.types.ArrayToScalarEdge(6)
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def testValue_Shape(self):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_shape_value: 5
            }
          }
        }
        decoding_steps {
          output_value_index: 1
        }
        """
    qvalue = arolla.types.ArrayShape(5)
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  @parameterized.parameters(
      ('unit', arolla.ARRAY_UNIT),
      ('boolean', arolla.ARRAY_BOOLEAN),
      ('bytes', arolla.ARRAY_BYTES),
      ('text', arolla.ARRAY_TEXT),
      ('int32', arolla.ARRAY_INT32),
      ('int64', arolla.ARRAY_INT64),
      ('uint64', arolla.types.ARRAY_UINT64),
      ('float32', arolla.ARRAY_FLOAT32),
      ('float64', arolla.ARRAY_FLOAT64),
      ('edge', arolla.ARRAY_EDGE),
      ('to_scalar_edge', arolla.ARRAY_TO_SCALAR_EDGE),
      ('shape', arolla.types.ARRAY_SHAPE),
  )
  def testQType(self, type_name, qvalue):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_%s_qtype: true
            }
          }
        }
        decoding_steps {
          output_value_index: 1
        }
        """ % type_name
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def _gen_error_proto(self, value_proto):
    return """
        version: 2
        decoding_steps {  # [0]
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {  # [1]
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {  # [2]
          codec {
            name: "arolla.serialization_codecs.OptionalV1Proto.extension"
          }
        }
        decoding_steps {  # [3]
          value {
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_unit_value { size: 5 }
            }
          }
        }
        decoding_steps {  # [4]
          value {
            [arolla.serialization_codecs.OptionalV1Proto.extension] {
              optional_unit_value: true
            }
          }
        }
        decoding_steps {  # [5]
          value {
            [arolla.serialization_codecs.OptionalV1Proto.extension] {
              optional_unit_qtype: true
            }
          }
        }
        decoding_steps {
          value {
            %s
          }
        }
        """ % value_proto

  def testError_MissingExtension(self):
    with self.assertRaisesRegex(ValueError, re.escape('no extension found;')):
      self.parse_container_text_proto(self._gen_error_proto('codec_index: 0'))

  def testError_MissingValue(self):
    with self.assertRaisesRegex(ValueError, re.escape('missing value;')):
      self.parse_container_text_proto(
          self._gen_error_proto(
              '[arolla.serialization_codecs.ArrayV1Proto.extension] { }'
          )
      )

  def testError_MissingSize(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('missing field array_unit_value.size;')
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { }'
              '}'
          )
      )

  def testError_NegativeSize(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected non-negative array_unit_value.size, got -5;'),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: -5 }'
              '}'
          )
      )

  def testError_ExpectedNoInputValues(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected no input_values, got 1;')
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: 3'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 0 }'
              '}'
          )
      )

  def testError_ExpectedNoIdsInEmptyArray(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected no array_unit_value.ids, got 3;')
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 0 ids: [1, 2, 3] }'
              '}'
          )
      )

  def testError_NoDenseData(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected DENSE_ARRAY_UNIT in input_values[0], got no value;'
        ),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 1 }'
              '}'
          )
      )

  def testError_BadDenseData(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected DENSE_ARRAY_UNIT in input_values[0], got QTYPE;'),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: 5'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 1 }'
              '}'
          )
      )

  def testError_TooManyItemsInDenseData(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected size of input_values[0] to be less-or-equal than 2,'
            ' got 5;'
        ),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 2 }'
              '}'
          )
      )

  def testError_TooManyInputValuesForDenseArray(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 1 item in input_values, got 2;')
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3, 4]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 5 }'
              '}'
          )
      )

  def testError_NoIdsForDenseArray(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected no array_unit_value.ids;')
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 5  ids: [ 0 ] }'
              '}'
          )
      )

  def testError_NoMissedIdValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected OPTIONAL_UNIT in input_values[1], got no value;'),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 10 }'
              '}'
          )
      )

  def testError_BadMissedIdValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected OPTIONAL_UNIT in input_values[1], got QTYPE;'),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3, 5]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 10 }'
              '}'
          )
      )

  def testError_TooManyInputValuesForPartialArray(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 2 items in input_values, got 3;')
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3, 4, 5]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 10 }'
              '}'
          )
      )

  def testError_MismatchedIdCount(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected 5 items in array_unit_value.ids, got 3;'),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3, 4]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 10  ids: [3, 2, 1] }'
              '}'
          )
      )

  def testError_NonMonotonicIds(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected a strictly monotonic sequence in array_unit_value.ids;'
        ),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3, 4]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 10  ids: [0, 2, 2, 3, 4] }'
              '}'
          )
      )

  def testError_NegativeId(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected array_unit_value.ids[0] to be non-negative, got -1;'
        ),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3, 4]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 10  ids: [-1, 1, 2, 3, 4] }'
              '}'
          )
      )

  def testError_LargeId(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected array_unit_value.ids[last] to be less-than 10, got 10;'
        ),
    ):
      self.parse_container_text_proto(
          self._gen_error_proto(
              'input_value_indices: [3, 4]'
              '[arolla.serialization_codecs.ArrayV1Proto.extension] {'
              '  array_unit_value { size: 10  ids: [0, 1, 2, 3, 10] }'
              '}'
          )
      )

  def testError_NegativeSizeShape(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected non-negative array_shape_value, got -4;'),
    ):
      self.parse_container_text_proto(self._gen_error_proto("""
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_shape_value: -4
          }
          """))

  def testError_NegativeSizeToScalarEdge(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected non-negative array_to_scalar_edge_value, got -3;'),
    ):
      self.parse_container_text_proto(self._gen_error_proto("""
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_to_scalar_edge_value: -3
          }
          """))

  def _gen_edge_error_proto(self, value_proto):
    return """
        version: 2
        decoding_steps {  # [0]
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {  # [1]
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {  # [2]
          value {
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_int64_value {
                size: 1
                values: 1
              }
            }
          }
        }
        decoding_steps {  # [3]
          value {
            input_value_indices: 2
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_int64_value {
                size: 1
              }
            }
          }
        }
        decoding_steps {
          value { %s }
        }
        """ % value_proto

  def testError_MissingEdgeTypeForEdge(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('missing field array_edge_value.edge_type;')
    ):
      self.parse_container_text_proto(self._gen_edge_error_proto("""
          input_value_indices: [3]
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_edge_value {}
          }
          """))

  def testError_MissingGroupSizeForEdge(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('missing field array_edge_value.parent_size;')
    ):
      self.parse_container_text_proto(self._gen_edge_error_proto("""
          input_value_indices: [3]
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_edge_value { edge_type: MAPPING }
          }
          """))

  def testError_NegativeGroupSizeForEdge(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected non-negative array_edge_value.parent_size, got -5;'
        ),
    ):
      self.parse_container_text_proto(self._gen_edge_error_proto("""
          input_value_indices: [3]
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_edge_value { edge_type: MAPPING parent_size: -5 }
          }
          """))

  def testError_NoArrayInputValueForEdge(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected ARRAY_INT64 in input_values[0], got DENSE_ARRAY_INT64;'
        ),
    ):
      self.parse_container_text_proto(self._gen_edge_error_proto("""
          input_value_indices: [2]
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_edge_value { edge_type: MAPPING parent_size: 1 }
          }
          """))

  def testError_TooManyInputValuesForEdge(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 1 item in input_values, got 2;')
    ):
      self.parse_container_text_proto(self._gen_edge_error_proto("""
          input_value_indices: [3, 3]
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_edge_value { edge_type: MAPPING parent_size: 1 }
          }
          """))

  @parameterized.parameters(
      arolla.array_float32([]),  # empty
      arolla.array_float32([], ids=[], size=10**5) | 0.5,  # const
      arolla.array_float32(range(10**5)),  # dense
      arolla.array_float32(  # sparse
          range(5), ids=[1, 10, 100, 1000, 10000], size=10**5
      ),
  )
  def testSmoke(self, qvalue):
    arolla.testing.assert_qvalue_allequal(
        arolla.s11n.loads(arolla.s11n.dumps(qvalue)), qvalue
    )

  def testValue_SparseArrayProto(self):
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 1
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_int32_value {
                size: 2
                values: [5, 4]
              }
            }
          }
        }
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.OptionalV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 3
            [arolla.serialization_codecs.OptionalV1Proto.extension] {
              optional_int32_value {}
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: [2, 4]
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_int32_value {
                size: 10
                ids: [1, 3]
              }
            }
          }
        }
        decoding_steps {
          output_value_index: 5
        }
        """
    qvalue = arolla.array(values=[5, 4], ids=[1, 3], size=10)
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    arolla.testing.assert_qvalue_allequal(actual_qvalue, qvalue)

  def testValue_ArrayEdge(self):
    qvalue = arolla.types.ArrayEdge.from_mapping(
        arolla.array_int64([0, 2, 3]), 4
    )
    text = """
        version: 2
        decoding_steps {
          codec { name: "arolla.serialization_codecs.ArrayV1Proto.extension" }
        }
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 1
            [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
              dense_array_int64_value {
                size: 3
                values: 0
                values: 2
                values: 3
              }
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: 2
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_int64_value {
                size: 3
              }
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: [3]
            codec_index: 0
            [arolla.serialization_codecs.ArrayV1Proto.extension] {
              array_edge_value { edge_type: MAPPING parent_size: 4 }
            }
          }
        }
        decoding_steps {
          output_value_index: 4
        }
        """
    self.assertDumpsEqual(qvalue, text)
    actual_qvalue = self.parse_container_text_proto(text)
    self.assertEqual(actual_qvalue.fingerprint, qvalue.fingerprint)

  @parameterized.parameters(
      arolla.types.ArrayEdge.from_split_points(arolla.array_int64([0, 2, 3])),
      arolla.types.ArrayEdge.from_mapping(arolla.array_int64([0, 2, 3]), 4),
  )
  def testSmokeForEdge(self, qvalue):
    roundtripped = typing.cast(
        arolla.types.ArrayEdge, arolla.s11n.loads(arolla.s11n.dumps(qvalue))
    )
    self.assertEqual(qvalue.fingerprint, roundtripped.fingerprint)


if __name__ == '__main__':
  absltest.main()
