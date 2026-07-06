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

import collections

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.operator_tests import backend_test_base


M = arolla.M


def gen_scalar_test_data():
  """Yields test data for strings.findall_regex.

  Yields: (test_description, arg1, arg2, result)
  """
  ScalarFindallRegexResult = collections.namedtuple(
      "ScalarFindallRegexResult",
      "matches, matches_size, groups_sizes",
  )

  def _yield_scalar_test_data(
      *,
      test_description: str,
      text: arolla.QValue,
      regex: str,
      result: ScalarFindallRegexResult,
  ):
    assert text.qtype in [arolla.TEXT, arolla.OPTIONAL_TEXT]
    yield (
        f"scalar_{test_description}",
        text,
        arolla.text(regex),
        arolla.tuple(
            arolla.dense_array(result.matches, value_qtype=arolla.TEXT),
            arolla.types.DenseArrayToScalarEdge(result.matches_size),
            arolla.types.DenseArrayEdge.from_sizes(result.groups_sizes),
        ),
    )

  yield from _yield_scalar_test_data(
      test_description="single_capturing_group_with_matches",
      text=arolla.text("5 bottles of beer tonight, no, 10 bottles of beer"),
      regex="(\\d+) bottles of beer",
      result=ScalarFindallRegexResult(
          matches=["5", "10"],
          matches_size=2,
          groups_sizes=[1, 1],
      ),
  )

  yield from _yield_scalar_test_data(
      test_description="no_matches",
      text=arolla.text("5 bottles of beer tonight, no, 10 bottles of beer"),
      regex="i match nothing",
      result=ScalarFindallRegexResult(
          matches=[],
          matches_size=0,
          groups_sizes=[],
      ),
  )

  yield from _yield_scalar_test_data(
      test_description="optional_text",
      text=arolla.optional_text("5 bottles of beer today, 10 bottles of beer"),
      regex="(\\d+) bottles of beer",
      result=ScalarFindallRegexResult(
          matches=["5", "10"],
          matches_size=2,
          groups_sizes=[1, 1],
      ),
  )

  yield from _yield_scalar_test_data(
      test_description="optional_text_none",
      text=arolla.optional_text(None),
      regex="(\\d+) bottles of beer",
      result=ScalarFindallRegexResult(
          matches=[],
          matches_size=0,
          groups_sizes=[],
      ),
  )


def gen_array_test_data():
  """Yields test data for strings.findall_regex.

  Yields: (test_description, arg1, arg2, result)
  """
  FindallRegexResult = collections.namedtuple(
      "FindallRegexResult", "matches, matches_sizes, groups_sizes"
  )

  def _yield_array_test_data(
      *,
      test_description: str,
      text: list[str | None],
      regex: str,
      result: FindallRegexResult,
  ):
    for array_name, array_factory, edge_factory in [
        ("array", arolla.array, arolla.types.ArrayEdge),
        ("dense_array", arolla.dense_array, arolla.types.DenseArrayEdge),
    ]:
      yield (
          f"{array_name}_{test_description}",
          array_factory(text),
          arolla.text(regex),
          arolla.tuple(
              array_factory(result.matches, value_qtype=arolla.TEXT),
              edge_factory.from_sizes(result.matches_sizes),
              edge_factory.from_sizes(result.groups_sizes),
          ),
      )

  yield from _yield_array_test_data(
      test_description="single_capturing_group_with_matches",
      text=[
          "I had 5 bottles of beer tonight",
          None,
          "foo",
          "100 bottles of beer, no, 1000 bottles of beer",
      ],
      regex="(\\d+) bottles of beer",
      result=FindallRegexResult(
          matches=["5", "100", "1000"],
          matches_sizes=[1, 0, 0, 2],
          groups_sizes=[1, 1, 1],
      ),
  )

  yield from _yield_array_test_data(
      test_description="multiple_capturing_groups_with_matches",
      text=[
          "I had 5 bottles of beer tonight",
          None,
          "foo",
          "100 bottles of beer, no, 1000 bottles of beer",
      ],
      regex="(\\d+) (bottles) (of) beer",
      result=FindallRegexResult(
          matches=[
              "5",
              "bottles",
              "of",
              "100",
              "bottles",
              "of",
              "1000",
              "bottles",
              "of",
          ],
          matches_sizes=[1, 0, 0, 2],
          groups_sizes=[3, 3, 3],
      ),
  )

  yield from _yield_array_test_data(
      test_description="no_matches",
      text=[
          "I had 5 bottles of beer tonight",
          None,
          "foo",
          "100 bottles of beer, no, 1000 bottles of beer",
      ],
      regex="i match nothing",
      result=FindallRegexResult(
          matches=[],
          matches_sizes=[0, 0, 0, 0],
          groups_sizes=[],
      ),
  )


TEST_DATA = tuple(gen_scalar_test_data()) + tuple(gen_array_test_data())
QTYPE_SIGNATURES = frozenset(
    (arg1.qtype, arg2.qtype, res.qtype) for _, arg1, arg2, res in TEST_DATA
)


class StringsFindallRegexTest(
    parameterized.TestCase, backend_test_base.SelfEvalMixin
):

  def test_qtype_signatures(self):
    self.require_self_eval_is_called = False
    arolla.testing.assert_qtype_signatures(
        M.strings.findall_regex, QTYPE_SIGNATURES
    )

  @parameterized.named_parameters(*TEST_DATA)
  def test_eval(self, arg1, arg2, expected_result):
    result = self.eval(M.strings.findall_regex(arg1, arg2))
    arolla.testing.assert_qvalue_equal_by_fingerprint(result, expected_result)


if __name__ == "__main__":
  absltest.main()
