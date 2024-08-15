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

"""Utils for dict operators testing."""

import collections
import itertools
from typing import Iterator

from arolla import arolla
from arolla.operator_tests import pointwise_test_utils

INF = float('inf')

DICT_KEY_QTYPES = (
    frozenset(arolla.types.SCALAR_QTYPES)
    - frozenset(arolla.types.FLOATING_POINT_QTYPES)
    - {arolla.UNIT}
)
DICT_VALUE_QTYPES = frozenset(arolla.types.SCALAR_QTYPES)

DICT_QTYPES = frozenset(
    arolla.types.make_dict_qtype(k, v)
    for (k, v) in itertools.product(DICT_KEY_QTYPES, DICT_VALUE_QTYPES)
)
# Dict implementation detail, used to test internal operators.
KEY_TO_ROW_DICT_QTYPES = frozenset(
    arolla.types.make_key_to_row_dict_qtype(k) for k in DICT_KEY_QTYPES
)

_DICT_KEYS_AND_MISSING_KEYS = (
    ([True], (None, False)),
    ([True, False], (None,)),
    ([1, 2, 3], (None, 4)),
    (['one', 'two', 'three'], (None, 'four')),
    ([b'one', b'two', b'three'], (None, 'four')),
)
_VALUES = (
    [True, None],
    [False, True, None],
    [3, 2, None],
    [1.5, 1.25, None],
    ['five', 'six', None],
    [b'five', b'six', None],
)
# Tuples of (dict, missing_keys).
# NOTE: for type deduction the tests assume that the first value is never
# missing.
_TEST_DATA = tuple(
    (dict(zip(keys, values)), missing_keys)
    for ((keys, missing_keys), values) in itertools.product(
        _DICT_KEYS_AND_MISSING_KEYS, _VALUES
    )
)

TestData = collections.namedtuple(
    'TestData', 'dictionary,missing_keys,key_qtype,value_qtype'
)


def _annotate_test_data_with_qtypes(test_data) -> Iterator[TestData]:
  """Annotates test data rows with all matching key/value QTypes."""
  unused_key_qtypes = set(DICT_KEY_QTYPES)
  unused_value_qtypes = set(DICT_VALUE_QTYPES)

  for dictionary, missing_keys in test_data:
    sample_key = next(iter(dictionary.keys()))
    key_qtypes = tuple(
        pointwise_test_utils.matching_qtypes(sample_key, DICT_KEY_QTYPES)
    )
    sample_value = next(iter(dictionary.values()))
    value_qtypes = tuple(
        pointwise_test_utils.matching_qtypes(sample_value, DICT_VALUE_QTYPES)
    )
    if not key_qtypes or not value_qtypes:
      raise RuntimeError(
          f'No test cases generated for test data ({dictionary}, '
          f'{missing_keys})'
      )

    for key_qtype in key_qtypes:
      unused_key_qtypes -= {key_qtype}
      for value_qtype in value_qtypes:
        unused_value_qtypes -= {value_qtype}
        yield TestData(dictionary, missing_keys, key_qtype, value_qtype)

  assert not unused_key_qtypes, f'Untested key QTypes {unused_key_qtypes}'
  assert not unused_value_qtypes, f'Untested value QTypes {unused_value_qtypes}'


# Tuples of (dict, missing_keys, key_qtype, value_qtype).
TEST_DATA = list(_annotate_test_data_with_qtypes(_TEST_DATA))
