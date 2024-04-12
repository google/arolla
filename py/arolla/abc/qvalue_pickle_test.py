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

"""Tests for pickling QValue objects.

The file is separated from qvalue_test.py because we want to use arolla.s11n
here.
"""

import pickle
from unittest import mock

from absl.testing import absltest
from absl.testing import parameterized
from arolla.abc import dummy_types
from arolla.abc import qtype as abc_qtype
from arolla.s11n import s11n


class QValuePickleTest(parameterized.TestCase):

  def test_pickle(self):
    pickled = pickle.dumps(abc_qtype.NOTHING)
    unpickled = pickle.loads(pickled)
    self.assertEqual(unpickled, abc_qtype.NOTHING)

  def test_no_codec(self):
    dummy_value = dummy_types.make_dummy_value()
    with self.assertRaisesRegex(
        ValueError, "specialization_key='::arolla::testing::DummyValue'"
    ):
      pickle.dumps(dummy_value)

  def test_arolla_unreduce_called(self):
    pickled = pickle.dumps(abc_qtype.NOTHING)
    serialized = s11n.dumps(abc_qtype.NOTHING)
    with mock.patch('arolla.abc.qtype.QValue', autospec=True) as mock_qvalue:
      pickle.loads(pickled)
      mock_qvalue._arolla_unreduce.assert_called_once_with(serialized)

  def test_arolla_unreduce(self):
    serialized = s11n.dumps(abc_qtype.NOTHING)
    self.assertEqual(
        abc_qtype.QValue._arolla_unreduce(serialized),  # type: ignore
        abc_qtype.NOTHING,
    )

    with self.assertRaisesRegex(TypeError, 'expected bytes, int found'):
      abc_qtype.QValue._arolla_unreduce(1)  # type: ignore

    with self.assertRaises(ValueError):
      abc_qtype.QValue._arolla_unreduce(b'ololo')  # type: ignore

    with self.assertRaisesRegex(
        ValueError, 'unexpected sizes in the serialized container'
    ):
      abc_qtype.QValue._arolla_unreduce(
          s11n.dumps_many(
              values=[abc_qtype.NOTHING, abc_qtype.NOTHING], exprs=[]
          )
      )  # type: ignore


if __name__ == '__main__':
  absltest.main()
