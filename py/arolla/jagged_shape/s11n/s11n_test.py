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

from absl.testing import absltest
from arolla import arolla
from arolla.jagged_shape import jagged_shape
from arolla.s11n.testing import codec_test_case

# Import protobufs for the codecs in use:
from arolla.jagged_shape.array.serialization_codecs import jagged_shape_codec_pb2 as _
from arolla.jagged_shape.dense_array.serialization_codecs import jagged_shape_codec_pb2 as _
from arolla.serialization_codecs.array import array_codec_pb2 as _
from arolla.serialization_codecs.dense_array import dense_array_codec_pb2 as _


class JaggedDenseArrayShapeS11NTest(codec_test_case.S11nCodecTestCase):

  def test_qtype(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension] {
            jagged_dense_array_shape_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    qtype = jagged_shape.JAGGED_DENSE_ARRAY_SHAPE
    self.assertDumpsEqual(qtype, text)
    self.assertLoadsEqual(text, qtype)

  def test_qvalue_rank_2(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension"
        }
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
              size: 2
              values: 0
              values: 2
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 2
          codec_index: 1
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_edge_value {
              edge_type: SPLIT_POINTS
            }
          }
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
          input_value_indices: 4
          codec_index: 1
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_edge_value {
              edge_type: SPLIT_POINTS
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 3
          input_value_indices: 5
          codec_index: 0
          [arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension] {
            jagged_dense_array_shape_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 6
      }
    """
    qvalue = jagged_shape.JaggedDenseArrayShape.from_edges(
        arolla.types.DenseArrayEdge.from_sizes([2]),
        arolla.types.DenseArrayEdge.from_sizes([2, 1]),
    )
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_qvalue_rank_0(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension] {
            jagged_dense_array_shape_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    qvalue = jagged_shape.JaggedDenseArrayShape.from_edges()
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_missing_value_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.JaggedDenseArrayShapeV1Proto.extension] {
          }
        }
      }
    """
    with self.assertRaisesRegex(ValueError, 'missing value'):
      self.parse_container_text_proto(text)


class JaggedArrayShapeS11NTest(codec_test_case.S11nCodecTestCase):

  def test_qtype(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension] {
            jagged_array_shape_qtype: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    qtype = jagged_shape.JAGGED_ARRAY_SHAPE
    self.assertDumpsEqual(qtype, text)
    self.assertLoadsEqual(text, qtype)

  def test_qvalue_rank_2(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.ArrayV1Proto.extension"
        }
      }
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.DenseArrayV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 2
          [arolla.serialization_codecs.DenseArrayV1Proto.extension] {
            dense_array_int64_value {
              size: 2
              values: 0
              values: 2
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 3
          codec_index: 1
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_int64_value {
              size: 2
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 4
          codec_index: 1
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_edge_value {
              edge_type: SPLIT_POINTS
            }
          }
        }
      }
      decoding_steps {
        value {
          codec_index: 2
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
          input_value_indices: 6
          codec_index: 1
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_int64_value {
              size: 3
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 7
          codec_index: 1
          [arolla.serialization_codecs.ArrayV1Proto.extension] {
            array_edge_value {
              edge_type: SPLIT_POINTS
            }
          }
        }
      }
      decoding_steps {
        value {
          input_value_indices: 5
          input_value_indices: 8
          codec_index: 0
          [arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension] {
            jagged_array_shape_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 9
      }
    """
    qvalue = jagged_shape.JaggedArrayShape.from_edges(
        arolla.types.ArrayEdge.from_sizes([2]),
        arolla.types.ArrayEdge.from_sizes([2, 1]),
    )
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_qvalue_rank_0(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          codec_index: 0
          [arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension] {
            jagged_array_shape_value: true
          }
        }
      }
      decoding_steps {
        output_value_index: 1
      }
    """
    qvalue = jagged_shape.JaggedArrayShape.from_edges()
    self.assertDumpsEqual(qvalue, text)
    self.assertLoadsEqual(text, qvalue)

  def test_missing_value_error(self):
    text = """
      version: 2
      decoding_steps {
        codec {
          name: "arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension"
        }
      }
      decoding_steps {
        value {
          [arolla.serialization_codecs.JaggedArrayShapeV1Proto.extension] {
          }
        }
      }
    """
    with self.assertRaisesRegex(ValueError, 'missing value'):
      self.parse_container_text_proto(text)


if __name__ == '__main__':
  absltest.main()
