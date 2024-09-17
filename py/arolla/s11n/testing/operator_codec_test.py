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
from arolla.while_loop import while_loop

# Import protobufs for the codecs in use:
from arolla.serialization_codecs.generic import operator_codec_pb2 as _
from arolla.serialization_codecs.generic import optional_codec_pb2 as _
from arolla.serialization_codecs.generic import scalar_codec_pb2 as _

M = arolla.M
P = arolla.P
L = arolla.L


class OperatorCodecTest(codec_test_case.S11nCodecTestCase):

  def testRegisteredOperator(self):
    value = M.math.add
    text = """
        version: 2
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "math.add"
            }
          }
        }
        decoding_steps {
          output_value_index: 1
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testLambdaOperator(self):
    v0 = arolla.LambdaOperator(
        '*args', P.args, name='v0'
    )  # Use 'v0' lambda like a universal value
    value = arolla.LambdaOperator(('x, y=', v0), v0(P.x, P.y), doc='doc-string')
    text = """
        version: 2
        decoding_steps {  # [0]
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {  # [1]
          placeholder_node { placeholder_key: "args" }
        }
        decoding_steps {  # [2]
          value {
            input_expr_indices: 1
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              lambda_operator {
                name: "v0"
                signature_spec: "*args"
              }
            }
          }
        }
        decoding_steps {  # [3]
          placeholder_node { placeholder_key: "x" }
        }
        decoding_steps {  # [4]
          placeholder_node { placeholder_key: "y" }
        }
        decoding_steps {  # [5]
          operator_node {
            operator_value_index: 2
            input_expr_indices: [3, 4]
          }
        }
        decoding_steps {  # [6]
          value {
            input_value_indices: 2
            input_expr_indices: 5
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              lambda_operator {
                name: "anonymous.lambda"
                signature_spec: "x, y="
                doc: "doc-string"
              }
            }
          }
        }
        decoding_steps {  # [7]
          output_value_index: 6
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testMakeTupleOperator(self):  # non-registered MakeTuple operator
    # Synthesize make_tuple operator:
    # The one of ways to get make_tuple() is unfolding a lambda operator
    # with a variadic argument.
    value = arolla.abc.to_lower_node(
        arolla.LambdaOperator('*args', P.args)(P.x)
    ).op
    self.assertEqual(value.qtype, arolla.OPERATOR)
    self.assertEqual('core.make_tuple', value.display_name)
    text = """
        version: 2
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              make_tuple_operator: true
            }
          }
        }
        decoding_steps {
          output_value_index: 1
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testGetNthOperator(self):  # non-registered GetNth operator
    value = arolla.types.GetNthOperator(2)
    text = """
        version: 2
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              get_nth_operator_index: 2
            }
          }
        }
        decoding_steps {
          output_value_index: 1
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testOverloadedOperator(self):
    value = arolla.OverloadedOperator(
        M.math.add, M.math.subtract, M.math.multiply, name='asm'
    )
    text = """
        version: 2
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "math.add"
            }
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "math.subtract"
            }
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "math.multiply"
            }
          }
        }
        decoding_steps {
          value {
            input_value_indices: [1, 2, 3]
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              overloaded_operator_name: "asm",
            }
          }
        }
        decoding_steps {
          output_value_index: 4
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testWhileLoopOperator(self):
    # A very simple loop:
    #   while x:
    #     x = x
    loop = while_loop.while_loop(
        initial_state=dict(x=L.a), condition=P.x, body=dict(x=P.x)
    )
    text = """
      version: 2
      decoding_steps {  # [0]
        codec {
          name: "arolla.serialization_codecs.OperatorV1Proto.extension"
        }
      }
      decoding_steps {  # [1]
        placeholder_node { placeholder_key: "loop_state" }
      }
      decoding_steps {  # [2]
        codec {
          name: "arolla.serialization_codecs.ScalarV1Proto.extension"
        }
      }
      decoding_steps {  # [3]
        value {
          codec_index: 2
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            int64_value: 0
          }
        }
      }
      decoding_steps {  # [4]
        literal_node {
          literal_value_index: 3
        }
      }
      decoding_steps {  # [5]
        value {
          codec_index: 0
          [arolla.serialization_codecs.OperatorV1Proto.extension] {
            registered_operator_name: "core.get_nth"
          }
        }
      }
      decoding_steps {  # [6]
        operator_node {
          operator_value_index: 5
          input_expr_indices: 1
          input_expr_indices: 4
        }
      }
      decoding_steps {  # [7]
        value {
          input_expr_indices: 6
          codec_index: 0
          [arolla.serialization_codecs.OperatorV1Proto.extension] {
            lambda_operator {
              name: "anonymous.loop_condition"
              signature_spec: "loop_state"
            }
          }
        }
      }
      decoding_steps {  # [8]
        value {
          codec_index: 2
          [arolla.serialization_codecs.ScalarV1Proto.extension] {
            text_value: "x"
          }
        }
      }
      decoding_steps {  # [9]
        literal_node {
          literal_value_index: 8
        }
      }
      decoding_steps {  # [10]
        value {
          codec_index: 0
          [arolla.serialization_codecs.OperatorV1Proto.extension] {
            registered_operator_name: "namedtuple.make"
          }
        }
      }
      decoding_steps {  # [11]
        operator_node {
          operator_value_index: 10
          input_expr_indices: 9
          input_expr_indices: 6,
        }
      }
      decoding_steps {  # [12]
        value {
          input_expr_indices: 11
          codec_index: 0
          [arolla.serialization_codecs.OperatorV1Proto.extension] {
            lambda_operator {
              name: "anonymous.loop_body"
              signature_spec: "loop_state"
            }
          }
        }
      }
      decoding_steps {  # [13]
        value {
          input_value_indices: 7
          input_value_indices: 12
          codec_index: 0
          [arolla.serialization_codecs.OperatorV1Proto.extension] {
            while_loop_operator {
              name: "anonymous.while_loop"
              signature_spec: "loop_state"
            }
          }
        }
      }
      decoding_steps {  # [14]
        output_value_index: 13
      }
    """
    self.assertDumpsEqual(loop.op, text)
    self.assertLoadsEqual(text, loop.op)

  def testBackendOperator(self):
    value = arolla.types.BackendOperator(
        name='foo.bar',
        signature=('x, y=, *z', 1.5),
        qtype_inference_expr=P.z,
        qtype_constraints=[
            (P.x == P.y, 'message {z}'),
        ],
        doc='doc-string',
    )
    text = """
        version: 2
        decoding_steps {  # [0]
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
              float32_value: 1.5
            }
          }
        }
        decoding_steps {  # [3]
          placeholder_node { placeholder_key: "x" }
        }
        decoding_steps {  # [4]
          placeholder_node { placeholder_key: "y" }
        }
        decoding_steps {  # [5]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.equal"
            }
          }
        }
        decoding_steps {  # [6]
          operator_node {
            operator_value_index: 5
            input_expr_indices: [3, 4]
          }
        }
        decoding_steps {  # [7]
          placeholder_node { placeholder_key: "z" }
        }
        decoding_steps {  # [8]
          value {
            codec_index: 0
            input_value_indices: 2
            input_expr_indices: 6
            input_expr_indices: 7
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              backend_operator {
                name: "foo.bar"
                signature_spec: "x, y=, *z"
                qtype_constraint_error_messages: [
                  "message {z}"
                ]
                doc: 'doc-string'
              }
            }
          }
        }
        decoding_steps {  # [9]
          output_value_index: 8
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testRestrictedLambdaOperator(self):
    value = arolla.types.RestrictedLambdaOperator(
        P.arg,
        qtype_constraints=[(P.arg == P.arg, 'message {arg}')],
        name='foo.bar',
        doc='doc-string',
    )
    text = """
        version: 2
        decoding_steps {  # [0]
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {  # [1]
          placeholder_node { placeholder_key: "arg" }
        }
        decoding_steps {  # [2]
          value {
            input_expr_indices: 1
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              lambda_operator {
                name: "foo.bar"
                signature_spec: "arg"
                doc: "doc-string"
              }
            }
          }
        }
        decoding_steps {  # [3]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.equal"
            }
          }
        }
        decoding_steps {  # [4]
          operator_node {
            operator_value_index: 3
            input_expr_indices: [1, 1]
          }
        }
        decoding_steps {  # [5]
          value {
            codec_index: 0
            input_value_indices: 2
            input_expr_indices: 4
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              restricted_lambda_operator {
                qtype_constraint_error_messages: "message {arg}"
              }
            }
          }
        }
        decoding_steps {  # [6]
          output_value_index: 5
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testDispatchOperator(self):
    value = arolla.types.DispatchOperator(
        'x,y,*z',
        name='foo.bar',
        eq_case=arolla.types.DispatchCase(M.math.add, condition=(P.x == P.y)),
        default=M.core.make_tuple,
    )
    text = """
        version: 2
        decoding_steps {  # [0]
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {  # [1]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "math.add"
            }
          }
        }
        decoding_steps {  # [2]
          leaf_node { leaf_key: "input_tuple_qtype" }
        }
        decoding_steps {  # [3]
          codec {
            name: "arolla.serialization_codecs.ScalarV1Proto.extension"
          }
        }
        decoding_steps {  # [4]
          value {
            codec_index: 3
            [arolla.serialization_codecs.ScalarV1Proto.extension] {
              int64_value: 0
            }
          }
        }
        decoding_steps {  # [5]
          literal_node {
            literal_value_index: 4
          }
        }
        decoding_steps {  # [6]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "qtype.get_field_qtype"
            }
          }
        }
        decoding_steps {  # [7]
          operator_node {
            operator_value_index: 6  # qtype.get_field_qtype
            input_expr_indices: 2  # input_tuple_qtype
            input_expr_indices: 5  # Literal(0)
          }
        }
        decoding_steps {  # [8]
          value {
            codec_index: 3
            [arolla.serialization_codecs.ScalarV1Proto.extension] {
              int64_value: 1
            }
          }
        }
        decoding_steps {  # [9]
          literal_node {
            literal_value_index: 8
          }
        }
        decoding_steps {  # [10]
          operator_node {
            operator_value_index: 6  # qtype.get_field_qtype
            input_expr_indices: 2  # input_tuple_qtype
            input_expr_indices: 9  # Literal(1)
          }
        }
        decoding_steps {  # [11]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.equal"
            }
          }
        }
        decoding_steps {  # [12]
          operator_node {
           operator_value_index: 11  # core.equal
             input_expr_indices: 7  # input_tuple_qtype[0]
             input_expr_indices: 10  # input_tuple_qtype[1]
           }
        }
        decoding_steps {  # [13]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.make_tuple"
            }
          }
        }
        decoding_steps {  # [14]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.presence_not"
            }
          }
        }
        decoding_steps {  # [15]
          operator_node {
            operator_value_index: 14  # core.presence_not
            input_expr_indices: 12  # input_tuple_qtype[0] == input_tuple_qtype[1]
          }
        }
        decoding_steps {  # [16]
          value {
            codec_index: 3
            [arolla.serialization_codecs.ScalarV1Proto.extension] {
              qtype_qtype: false
            }
          }
        }
        decoding_steps {  # [17]
          literal_node {
            literal_value_index: 16  # NOTHING
          }
        }
        decoding_steps {  # [18]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.not_equal"
            }
          }
        }
        decoding_steps {  # [19]
          operator_node {
            operator_value_index: 18  # core.not_equal
            input_expr_indices: 7  # input_tuple_qtype[0]
            input_expr_indices: 17  # Literal(NOTHING)
          }
        }
        decoding_steps {  # [20]
          operator_node {
            operator_value_index: 18  # core.not_equal
            input_expr_indices: 10  # input_tuple_qtype[1]
            input_expr_indices: 17  # Literal(NOTHING)
          }
        }
        decoding_steps {  # [21]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.presence_and"
            }
          }
        }
        decoding_steps {  # [22]
          operator_node {
            operator_value_index: 21  # core.presence_and
            input_expr_indices: 19  # input_tuple_qtype[0] != arolla.NOTHING
            input_expr_indices: 20  # input_tuple_qtype[1] != arolla.NOTHING
          }
        }
        decoding_steps {  # [23]
          value {
            input_value_indices: 1  # math.add
            input_value_indices: 13  # core.make_tuple
            input_expr_indices: 12  # input_tuple_qtype[0] == input_tuple_qtype[1]
            input_expr_indices: 15  # core.presence_not(#10)
            input_expr_indices: 22  # dispatch readyness condition
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              dispatch_operator {
                name: "foo.bar"
                signature_spec: "x, y, *z"
                overload_names: "eq_case"
                overload_names: "default"
              }
            }
          }
        }
        decoding_steps {  # [24]
          output_value_index: 23
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testDummyOperator(self):
    value = arolla.types.DummyOperator(
        name='foo.bar',
        signature=('x, y=, *z', 1.5),
        result_qtype=arolla.TEXT,
        doc='doc-string',
    )
    text = """
        version: 2
        decoding_steps {  # [0]
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
              float32_value: 1.5
            }
          }
        }
        decoding_steps {  # [3]
          value {
            codec_index: 1
            [arolla.serialization_codecs.ScalarV1Proto.extension] {
              text_qtype: true
            }
          }
        }
        decoding_steps {  # [4]
          value {
            input_value_indices: 2
            input_value_indices:3
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              dummy_operator {
                name: "foo.bar"
                signature_spec: "x, y=, *z"
                doc: "doc-string"
              }
            }
          }
        }
        decoding_steps {  # [5]
          output_value_index: 4
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testGenericOperator(self):
    v0 = M.core.identity  # Use 'core.identity' as a value
    value = arolla.types.GenericOperator(
        name='foo.bar', signature=('x, y=', v0), doc='doc-string'
    )
    text = """
        version: 2
        decoding_steps {  # [0]
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {  # [1]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: 'core.identity'
            }
          }
        }
        decoding_steps {  # [2]
          value {
            input_value_indices: 1
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              generic_operator {
                name: "foo.bar"
                signature_spec: "x, y="
                doc: "doc-string"
              }
            }
          }
        }
        decoding_steps {  # [3]
          output_value_index: 2
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testGenericOperatorOverload(self):
    value = arolla.types.GenericOperatorOverload(
        M.core.make_tuple,
        overload_condition_expr=(L.input_tuple_qtype == L.input_tuple_qtype),
    )
    text = """
        version: 2
        decoding_steps {  # [0]
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {  # [1]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.make_tuple"
            }
          }
        }
        decoding_steps {  # [2]
          leaf_node {
            leaf_key: "input_tuple_qtype"
          }
        }
        decoding_steps {  # [3]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.equal"
            }
          }
        }
        decoding_steps {  # [4]
          operator_node {
            operator_value_index: 3  # core.equal
            input_expr_indices: 2  # input_tuple_qtype
            input_expr_indices: 2  # input_tuple_qtype
          }
        }
        decoding_steps {  # [5]
          codec {
            name: "arolla.serialization_codecs.OptionalV1Proto.extension"
          }
        }
        decoding_steps {  # [6]
          value {
            codec_index: 5
            [arolla.serialization_codecs.OptionalV1Proto.extension] {
              optional_unit_value: true
            }
          }
        }
        decoding_steps {  # [7]
          literal_node {
            literal_value_index: 6  # true
          }
        }
        decoding_steps {  # [8]
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              registered_operator_name: "core.presence_and"
            }
          }
        }
        decoding_steps {  # [9]
          operator_node {
            operator_value_index: 8  # core.presence_and
            input_expr_indices: 4  # input_tuple_qtype == input_tuple_qtype
            input_expr_indices: 7  # true
          }
        }
        decoding_steps {  # [10]
          value {
            input_value_indices: 1  # core.make_tuple
            input_expr_indices: 9  # (input_tuple_qtype == input_tuple_qtype) & true
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              generic_operator_overload { }
            }
          }
        }
        decoding_steps {  # [11]
          output_value_index: 10
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)

  def testError_WhileLoopOperatorMissingName(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing while_loop_operator.name; value=WHILE_LOOP_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: "math.add"
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: 1
              input_value_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                while_loop_operator {
                  signature_spec: "loop_state"
                }
              }
            }
          }
          """)

  def testError_WhileLoopOperatorMissingSignature(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing while_loop_operator.signature_spec;'
            ' value=WHILE_LOOP_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: "math.add"
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: 1
              input_value_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                while_loop_operator {
                  name: "anonymous.while_loop"
                }
              }
            }
          }
          """)

  def testError_WhileLoopOperatorMissingInput(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('expected 2 input values, got 1; value=WHILE_LOOP_OPERATOR;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: "math.add"
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                while_loop_operator {
                  name: "anonymous.while_loop"
                  signature_spec: "loop_state"
                }
              }
            }
          }
          """)

  def testError_WhileLoopOperatorIncorrectInputType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected EXPR_OPERATOR in input_values[0], got BOOLEAN;'
            ' value=WHILE_LOOP_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
                boolean_value: false
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: 2
              input_value_indices: 2
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                while_loop_operator {
                  name: "anonymous.while_loop"
                  signature_spec: "loop_state"
                }
              }
            }
          }
          """)

  def testError_MissingExtension(self):
    with self.assertRaisesRegex(ValueError, re.escape('no extension found;')):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              codec_index: 0
            }
          }
          """)

  def testError_MissingValue(self):
    with self.assertRaisesRegex(ValueError, re.escape('missing value;')):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] { }
            }
          }
          """)

  def testError_LambdaOperatorMissingName(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('missing lambda_operator.name; value=LAMBDA_OPERATOR;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                lambda_operator { }
              }
            }
          }
          """)

  def testError_LambdaOperatorMissingSignatureSpec(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing lambda_operator.signature_spec; value=LAMBDA_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                lambda_operator {
                  name: 'xyz'
                }
              }
            }
          }
          """)

  def testError_LambdaOperatorMissingLambdaBody(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing input_expr_index for lambda body; value=LAMBDA_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                lambda_operator {
                  name: 'xyz'
                  signature_spec: ''
                }
              }
            }
          }
          """)

  def testError_OverloadedOperatorInvalidInputValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected EXPR_OPERATOR, got a QTYPE value as an input;'
            ' value=OVERLOADED_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                operator_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: [1]
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                overloaded_operator_name: "xyz"
              }
            }
          }
          """)

  def testError_BackendOperatorMissingName(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('missing backend_operator.name; value=BACKEND_OPERATOR;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                backend_operator { }
              }
            }
          }
          """)

  def testError_BackendOperatorMissingSignatureSpec(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing backend_operator.signature_spec; value=BACKEND_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                backend_operator {
                  name: 'xyz'
                }
              }
            }
          }
          """)

  def testError_BackendOperatorInputExprCount(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected 1 input_expr_index, got 0; value=BACKEND_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                backend_operator {
                  name: 'xyz'
                  signature_spec: ''
                }
              }
            }
          }
          """)

  def testError_BackendOperatorBadSignature(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "default value expected, but not provided for parameter: 'x';"
            ' value=BACKEND_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            placeholder_node { placeholder_key: "x" }
          }
          decoding_steps {
            value {
              input_expr_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                backend_operator {
                  name: 'xyz'
                  signature_spec: 'x='
                }
              }
            }
          }
          """)

  def testError_RestrictedLambdaOperatorMissingValueIndex(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing input_value_index for base lambda operator;'
            ' value=RESTRICTED_LAMBDA_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                restricted_lambda_operator { }
              }
            }
          }
          """)

  def testError_RestrictedLambdaOperatorManyValueIndices(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected 1 input_value_index, got 2;'
            ' value=RESTRICTED_LAMBDA_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
                int64_value: 0
              }
            }
          }
          decoding_steps {  # [3]
            value {
              input_value_indices: [2, 2]
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                restricted_lambda_operator { }
              }
            }
          }
          """)

  def testError_RestrictedLambdaOperatorWrongLambdaQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected EXPR_OPERATOR as input value, got INT64;'
            ' value=RESTRICTED_LAMBDA_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
                int64_value: 0
              }
            }
          }
          decoding_steps {  # [3]
            value {
              input_value_indices: 2
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                restricted_lambda_operator { }
              }
            }
          }
          """)

  def testError_RestrictedLambdaOperatorWrongLambdaKind(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected lambda operator as input value, got <RegisteredOperator'
            " 'math.add'>; value=RESTRICTED_LAMBDA_OPERATOR;"
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: "math.add"
              }
            }
          }
          decoding_steps {  # [2]
            value {
              input_value_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                restricted_lambda_operator { }
              }
            }
          }
          """)

  def testError_RestrictedLambdaOperatorMissingExprIndices(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected 2 input_expr_index, got 0;'
            ' value=RESTRICTED_LAMBDA_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            placeholder_node { placeholder_key: "arg" }
          }
          decoding_steps {  # [2]
            value {
              input_expr_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                lambda_operator {
                  name: "foo.bar"
                  signature_spec: "arg"
                }
              }
            }
          }
          decoding_steps {  # [3]
            value {
              input_value_indices: 2
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                restricted_lambda_operator {
                  qtype_constraint_error_messages: ['foo', 'bar']
                }
              }
            }
          }
          """)

  def testError_DispatchOperatorMissingName(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('missing dispatch_operator.name; value=DISPATCH_OPERATOR;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: "math.add"
              }
            }
          }
          decoding_steps {  # [2]
            value {
              input_value_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dispatch_operator {
                  signature_spec: "x, y, *z"
                  overload_names: "eq_case"
                  overload_names: "default_case"
                }
              }
            }
          }
          """)

  def testError_DispatchOperatorMissingSignatureSpec(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing dispatch_operator.signature_spec; value=DISPATCH_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: "math.add"
              }
            }
          }
          decoding_steps {  # [2]
            value {
              input_value_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dispatch_operator {
                  name: "foo.bar"
                  overload_names: "eq_case"
                  overload_names: "default_case"
                }
              }
            }
          }
          """)

  def testError_DispatchOperatorMissingOverloads(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('missing overloads; value=DISPATCH_OPERATOR;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dispatch_operator {
                  name: "foo.bar"
                  signature_spec: "x, y"
                }
              }
            }
          }
          """)

  def testError_DispatchOperatorWrongNumberOfValues(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected input_values.size() + 1 == input_exprs.size(), got 2 and'
            ' 1; value=DISPATCH_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: "math.add"
              }
            }
          }
          decoding_steps {  # [2]
            placeholder_node { placeholder_key: "x" }
          }
          decoding_steps {  # [3]
            placeholder_node { placeholder_key: "y" }
          }
          decoding_steps {  # [4]
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: "core.equal"
              }
            }
          }
          decoding_steps {  # [5]
            operator_node {
              operator_value_index: 4
              input_expr_indices: 2
              input_expr_indices: 3
            }
          }
          decoding_steps {  # [6]
            value {
              input_value_indices: 1
              input_value_indices: 1
              input_expr_indices: 5
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dispatch_operator {
                  name: "foo.bar"
                  signature_spec: "x, y"
                  overload_names: "eq_case"
                  overload_names: "default_case"
                }
              }
            }
          }
          """)

  def testError_DispatchOperatorMissingCaseNames(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected input_values.size() =='
            ' dispatch_operator_proto.overload_names_size(), got 1 and 0;'
            ' value=DISPATCH_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
                float32_value: 1.5
              }
            }
          }
          decoding_steps {  # [3]
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                boolean_value: true
              }
            }
          }
          decoding_steps {  # [4]
            literal_node {
              literal_value_index: 3
            }
          }
          decoding_steps {  # [5]
            value {
              input_value_indices: 2
              input_expr_indices: 4
              input_expr_indices: 4
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dispatch_operator {
                  name: "foo.bar"
                  signature_spec: "x, y"
                }
              }
            }
          }
          """)

  def testError_DispatchOperatorWrongValueType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected EXPR_OPERATOR as 0-th input value, got FLOAT32;'
            ' value=DISPATCH_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
                float32_value: 1.5
              }
            }
          }
          decoding_steps {  # [3]
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                boolean_value: true
              }
            }
          }
          decoding_steps {  # [4]
            literal_node {
              literal_value_index: 3
            }
          }
          decoding_steps {  # [5]
            value {
              input_value_indices: 2
              input_expr_indices: 4
              input_expr_indices: 4
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dispatch_operator {
                  name: "foo.bar"
                  signature_spec: "x, y"
                  overload_names: "eq_case"
                }
              }
            }
          }
          """)

  def testError_DummyOperatorMissingName(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('missing dummy_operator.name; value=DUMMY_OPERATOR;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dummy_operator { }
              }
            }
          }
          """)

  def testError_DummyOperatorMissingSignatureSpec(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing dummy_operator.signature_spec; value=DUMMY_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dummy_operator {
                  name: 'xyz'
                }
              }
            }
          }
          """)

  def testError_DummyOperatorTooFewValues(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected at least one input_value_index, got 0;'
            ' value=DUMMY_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dummy_operator {
                  name: 'xyz'
                  signature_spec: 'x, y=, *z'
                }
              }
            }
          }
          """)

  def testError_DummyOperatorNotQType(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected the last input_value_index to be a QType, got TEXT;'
            ' value=DUMMY_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
                text_value: ""
              }
            }
          }
          decoding_steps {  # [4]
            value {
              input_value_indices: 2
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dummy_operator {
                  name: 'xyz'
                  signature_spec: 'x='
                }
              }
            }
          }
          """)

  def testError_DummyOperatorBadSignature(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "default value expected, but not provided for parameter: 'x';"
            ' value=DUMMY_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
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
                text_qtype: true
              }
            }
          }
          decoding_steps {
            value {
              input_value_indices: 2
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                dummy_operator {
                  name: 'xyz'
                  signature_spec: 'x='
                }
              }
            }
          }
          """)

  def testError_GenericOperatorMissingName(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('missing generic_operator.name; value=GENERIC_OPERATOR;'),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                generic_operator {
                  signature_spec: 'x'
                }
              }
            }
          }
          """)

  def testError_GenericOperatorMissingSignature(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'missing generic_operator.signature_spec; value=GENERIC_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                generic_operator {
                  name: 'foo.bar'
                }
              }
            }
          }
          """)

  def testError_GenericOperatorMissingDefaultValue(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "default value expected, but not provided for parameter: 'x';"
            ' value=GENERIC_OPERATOR;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                generic_operator {
                  name: 'foo.bar'
                  signature_spec: 'x='
                }
              }
            }
          }
          """)

  def testError_GenericOperatorOverloadNoBaseOperator(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected 1 input value, got 0; value=GENERIC_OPERATOR_OVERLOAD;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            leaf_node { leaf_key: "input_tuple_qtype" }
          }
          decoding_steps {  # [2]
            value {
              input_expr_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                generic_operator_overload { }
              }
            }
          }
          """)

  def testError_GenericOperatorOverloadBadBaseOperator(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected EXPR_OPERATOR as input value, got FLOAT32;'
            ' value=GENERIC_OPERATOR_OVERLOAD;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            leaf_node { leaf_key: "input_tuple_qtype" }
          }
          decoding_steps {  # [2]
            codec {
              name: "arolla.serialization_codecs.ScalarV1Proto.extension"
            }
          }
          decoding_steps {  # [3]
            value {
              [arolla.serialization_codecs.ScalarV1Proto.extension] {
                float32_value: 0.0
              }
            }
          }
          decoding_steps {  # [4]
            value {
              input_value_indices: 3
              input_expr_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                generic_operator_overload { }
              }
            }
          }
          """)

  def testError_GenericOperatorOverloadMissingExpr(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected 1 input expr, got 0; value=GENERIC_OPERATOR_OVERLOAD;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: 'core.make_tuple'
              }
            }
          }
          decoding_steps {  # [2]
            value {
              input_value_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                generic_operator_overload { }
              }
            }
          }
          """)

  def testError_GenericOperatorOverloadBadCondition(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'prepared overload condition contains unexpected placeholders: P.x;'
            ' value=GENERIC_OPERATOR_OVERLOAD;'
        ),
    ):
      self.parse_container_text_proto("""
          version: 2
          decoding_steps {  # [0]
            codec {
              name: "arolla.serialization_codecs.OperatorV1Proto.extension"
            }
          }
          decoding_steps {  # [1]
            placeholder_node { placeholder_key: 'x' }
          }
          decoding_steps {  # [2]
            value {
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                registered_operator_name: 'core.make_tuple'
              }
            }
          }
          decoding_steps {  # [3]
            value {
              input_value_indices: 2
              input_expr_indices: 1
              [arolla.serialization_codecs.OperatorV1Proto.extension] {
                generic_operator_overload { }
              }
            }
          }
          """)

  def testOperatorQType(self):
    value = arolla.OPERATOR
    text = """
        version: 2
        decoding_steps {
          codec {
            name: "arolla.serialization_codecs.OperatorV1Proto.extension"
          }
        }
        decoding_steps {
          value {
            codec_index: 0
            [arolla.serialization_codecs.OperatorV1Proto.extension] {
              operator_qtype: true
            }
          }
        }
        decoding_steps {
          output_value_index: 1
        }
    """
    self.assertDumpsEqual(value, text)
    self.assertLoadsEqual(text, value)


if __name__ == '__main__':
  absltest.main()
