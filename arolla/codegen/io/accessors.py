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

"""Library with a variety of accessors implementations."""

from typing import List, Tuple, Set, Optional

from arolla.codegen.io import cpp


def _prettify_lambda(lambda_str: str) -> str:
  """Removes empty lines and extra spaces at the end.

  Useful to have golden files pass basic text file requirements.

  Args:
    lambda_str: text of the lambda function

  Returns:
    Formatted string to pass basic text file style requirements.
  """
  return '\n'.join(l.rstrip() for l in lambda_str.split('\n') if l.strip())


class Accessor:
  """Holds code used to read from or write to an arbitrary data structure.

  E.g., a field of a struct or Protocol Message.
  """

  def __init__(self,
               lambda_str: str,
               includes: List[cpp.Include],
               default_name: Optional[str] = None):
    # pyformat: disable
    """Constructs accessor from lambda body and required headers.

    Args:
      lambda_str: c++ code for the lambda.
        There are some examples:
          * single argument function returning the field value associated
            with this Accessor. E.g., "[](const auto& i) { return i + 1; }"
          * two argument function returning the field value by given key
            associated with this Accessor.
            E.g., "[](const auto& m, const std::string& key) { return m[key]; }"
          * the constexpr function with single meta function argument,
            returning not tempated function to mutate the value,
            E.g., "[](auto output_type_meta_fn) {
                using output_type = typename decltype(
                    output_type_meta_fn)::type;
                return [](const decltype(std::declval<output_type>().a)& value,
                          output_type* out) {
                  out->a = value;
                };
            }"
      includes: list of cpp.Include with required header files.
      default_name: default name for this accessor.
        Used as a name if not specified by AccessorsList.
        If None, name must be specified explicitly in AccessorsList.
    """
    # pyformat: enable
    self._lambda_str = _prettify_lambda(lambda_str)
    self._includes = set(includes)
    self._default_name = default_name

  @property
  def required_includes(self) -> Set[cpp.Include]:
    """Returns set of required cpp.Include."""
    return self._includes

  @property
  def lambda_str(self) -> str:
    """Returns full definition of polymorphic lambda to access the field."""
    return self._lambda_str

  @property
  def default_name(self) -> str:
    """Returns name to be used by this accessor if not other specified."""
    if not self._default_name:
      raise ValueError('Default name is not specified for %s' % type(self))
    return self._default_name

  def __eq__(self, other) -> bool:
    return isinstance(
        other, Accessor) and (self._default_name == other._default_name and
                              self.lambda_str == other.lambda_str and
                              self.required_includes == other.required_includes)


def sorted_accessor_list(
    name_accessors: List[Tuple[str, Accessor]]) -> List[Tuple[str, Accessor]]:
  """Sort and check duplicated names of the accessors."""
  result = sorted(
      [(name or acc.default_name, acc) for name, acc in name_accessors],
      key=lambda name_acc: name_acc[0])
  duplicate_names = set()
  result_no_duplicates = result[:1]
  for (cur_name, cur), (nxt_name, nxt) in zip(result, result[1:]):
    if cur_name == nxt_name:
      if cur != nxt:
        duplicate_names.add(cur_name)
    else:
      result_no_duplicates.append((nxt_name, nxt))
  if duplicate_names:
    raise ValueError('Duplicate accessor names: %s' % sorted(duplicate_names))
  return result_no_duplicates


def split_accessors_into_shards(
    accessors_list: List[Tuple[str, Accessor]], shard_count: int):
  """Yields up to shard_count (w/o empty) sublists with +-1 equal length."""
  if shard_count <= 1:
    yield accessors_list
    return

  accessors_per_batch = len(accessors_list) // shard_count
  # first shards will be 1 element longer
  larger_shards = len(accessors_list) - accessors_per_batch * shard_count
  start = 0
  for i in range(shard_count):
    length = accessors_per_batch + (1 if i < larger_shards else 0)
    if length > 0:
      yield accessors_list[start : start + length]
    start += length

  assert start == len(accessors_list)


class AccessorsList:
  """Represents list of accessors."""

  def __init__(self, name_accessors: List[Tuple[str, Accessor]]):
    """Constructs from named Accessors. Empty names replaced by default_name."""
    self._accessors = sorted_accessor_list(name_accessors)

  @property
  def names(self) -> List[str]:
    """Returns sorted list of names of accessors."""
    return [name for name, _ in self._accessors]

  @property
  def accessors(self) -> List[Accessor]:
    """Returns list of accessors sorted by name."""
    return [accessor for _, accessor in self._accessors]

  def __len__(self) -> int:
    """Returns number of accessors."""
    return len(self._accessors)

  def shards(self, max_shard_size: int) -> List['AccessorsList']:
    """Shard list into several lists with given maximum size."""
    result = []
    cur_list = []
    for accessor in self._accessors:
      if len(cur_list) == max_shard_size:
        result.append(AccessorsList(cur_list))
        cur_list = []
      cur_list.append(accessor)
    result.append(AccessorsList(cur_list))
    return result

  @property
  def required_includes(self) -> List[cpp.Include]:
    """Returns union sorted list of cpp.Include required for all accessors."""
    includes = set()
    for _, accessor in self._accessors:
      includes = includes.union(accessor.required_includes)
    return sorted(includes)


def path_accessor(path: str, default_name: Optional[str] = None) -> Accessor:
  """Returns accessor by appending path to the input.

  Args:
    path: str to append after parameter name to get the field, e.g., `.x()`.
    default_name: default name for the accessor. Used as a name if not specified
      by AccessorsList.

  Returns:
    Accessor with no required includes.

  Examples:
    struct InputType {
      int x;
      std::vector<std::pair<int, int>> v;
     };
     ".x" gives field `x`
     ".v.size()" gives length of the field v
     ".v.back().first" gives the first element of the last pair in field v
     ".x & 1" gives the last bit of the field x
  """
  return Accessor('[](const auto& input) { return input%s; }' % path, [],
                  default_name)
