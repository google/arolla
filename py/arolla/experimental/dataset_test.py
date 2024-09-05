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
from arolla.experimental import dataset as arolla_dataset

M = arolla.OperatorsContainer()


class DataSetTest(absltest.TestCase):

  def test_simple_computation(self):
    ds = arolla_dataset.DataSet()
    ds.x = 2.718
    ds.y = 3.141
    ds.z = arolla.array([1.0, 2.0, 3.0])
    ds.values = ds.eval(
        M.core.const_like(ds.vars.z, ds.vars.x**ds.vars.y) + ds.vars.z
    )
    ds.expected_values = arolla.array([24.11945, 25.11945, 26.11945])
    err = ds.eval(
        M.core.presence_or(
            M.math.sum(M.math.abs(ds.vars.values - ds.vars.expected_values)),
            -1.0,
        )
    )
    arolla.testing.assert_qvalue_allclose(err, arolla.float32(0.0), atol=1e-5)


if __name__ == '__main__':
  absltest.main()
