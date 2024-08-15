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

"""Tests for M.core.make_slice."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla

M = arolla.M

TEST_DATA = (
    {},
    dict(start=arolla.unit()),
    dict(stop=arolla.unit()),
    dict(step=arolla.unit()),
    dict(start=arolla.unit(), stop=arolla.boolean(False)),
    dict(
        start=arolla.unit(),
        stop=arolla.boolean(False),
        step=arolla.bytes(b'foo'),
    ),
)


class NamedtupleMakeTest(parameterized.TestCase):

  @parameterized.parameters(*TEST_DATA)
  def test_value(self, **fields):
    slice_qvalue = arolla.eval(M.core.make_slice(**fields))
    self.assertEqual(
        slice_qvalue.qtype,
        arolla.eval(
            M.qtype.make_slice_qtype(**{k: v.qtype for k, v in fields.items()})
        ),
    )
    raw_tuple = arolla.eval(
        M.derived_qtype.upcast(slice_qvalue.qtype, slice_qvalue)
    )
    tuple_qvalue = arolla.tuple(
        fields.get('start', arolla.unspecified()),
        fields.get('stop', arolla.unspecified()),
        fields.get('step', arolla.unspecified()),
    )
    arolla.testing.assert_qvalue_equal_by_fingerprint(raw_tuple, tuple_qvalue)


if __name__ == '__main__':
  absltest.main()
