# Copyright 2025 Google LLC
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
from arolla.abc import abc as arolla_abc
from arolla.expr import expr as arolla_expr
from arolla.expr import source_location
from arolla.testing import testing as arolla_testing
from arolla.types import types as arolla_types


M = arolla_expr.OperatorsContainer()
P = arolla_expr.PlaceholderContainer()


class SourceLocationTest(absltest.TestCase):

  def test_strip_source_locations(self):
    z = M.core.presence_and(P.x, P.y)

    decayed_source_location = arolla_abc.decay_registered_operator(
        M.annotation.source_location
    )

    annotated = decayed_source_location(
        M.annotation.source_location(
            z,
            arolla_types.namedtuple(
                function_name='inner',
                file_name='file.py',
                line=2,
                column=0,
                line_text='inner line',
            ),
        ),
        arolla_types.namedtuple(
            function_name='outer',
            file_name='file.py',
            line=1,
            column=0,
            line_text='outer line',
        ),
    )
    stripped = source_location.strip_source_locations(annotated)

    arolla_testing.assert_expr_equal_by_fingerprint(stripped, z)


if __name__ == '__main__':
  absltest.main()
