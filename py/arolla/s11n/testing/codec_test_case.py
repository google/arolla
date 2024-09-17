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

"""Helpers for serialization codec testing."""

import collections.abc

from absl.testing import parameterized
from arolla import arolla
from google.protobuf import text_format
from google.protobuf import unknown_fields

# Import protobufs for the codecs in use:
from arolla.serialization_base import base_pb2


class S11nCodecTestCase(parameterized.TestCase):
  """Extension of parameterized.TestCase with methods for codec testing."""

  def parse_container_text_proto(self, input_text_format: str):
    """Returns the decoded value."""
    container_proto = text_format.Parse(
        input_text_format, base_pb2.ContainerProto()
    )
    return arolla.s11n.loads(container_proto.SerializeToString())

  def assert_proto2_equal(self, actual_message, expected_message):
    """Compare the messages using deterministic serialization."""
    actual_data = actual_message.SerializeToString(deterministic=True)
    expected_data = expected_message.SerializeToString(deterministic=True)
    if actual_data == expected_data:
      return
    # At this point, we know the messages are not equal. We use a string
    # comparison to generate an error with a diff. Different prefixes are
    # added to the actual and expected values to ensure the assertion never
    # matches
    #
    # Note: There is a known limitation with signed NaN values: if one message
    # contains a positive NaN and another contains a negative NaN, the text
    # diff will not reflect any difference.
    actual_text = text_format.MessageToString(actual_message, as_utf8=True)
    expected_text = text_format.MessageToString(expected_message, as_utf8=True)
    self.assertMultiLineEqual(
        'actual:\n' + actual_text, 'expected:\n' + expected_text
    )

  def assert_proto2_no_unknown_fields(self, message):
    """Check that there are no unknown fields within the message."""

    def dfs(msg):
      if hasattr(msg, 'UnknownFields'):
        if len(unknown_fields.UnknownFieldSet(msg)) > 0:  # pylint: disable=g-explicit-length-test
          raise RuntimeError(
              'Unknown fields in the message. It could happen '
              'if some codecs were not loaded in Python.'
          )
      if isinstance(msg, collections.abc.MutableSequence):
        # Check repeated fields
        for fld in msg:
          dfs(fld)
      elif hasattr(msg, 'ListFields'):
        # Check message fields
        for _, fld in msg.ListFields():
          dfs(fld)

    return dfs(message)

  def assertDumpsEqual(
      self, input_qvalue: arolla.QValue, expected_text_format: str
  ):
    """Assertion check that `input_qvalue` gets serialized in expected way."""
    actual_value = base_pb2.ContainerProto()
    actual_value.ParseFromString(arolla.s11n.dumps(input_qvalue))
    expected_value = text_format.Parse(
        expected_text_format, base_pb2.ContainerProto()
    )
    self.assert_proto2_no_unknown_fields(actual_value)
    self.assert_proto2_equal(actual_value, expected_value)

  def assertLoadsEqual(
      self, input_text_format: str, expected_qvalue: arolla.QValue
  ):
    """Assertion check that deserialization produces `expected_qvalue`."""
    actual_qvalue = self.parse_container_text_proto(input_text_format)
    self.assertIsInstance(actual_qvalue, arolla.QValue)
    self.assertEqual(expected_qvalue.qtype, actual_qvalue.qtype)  # pytype: disable=attribute-error
    self.assertEqual(expected_qvalue.fingerprint, actual_qvalue.fingerprint)
