// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
#ifndef AROLLA_QEXPR_OPERATORS_STRINGS_FINDALL_REGEX_H_
#define AROLLA_QEXPR_OPERATORS_STRINGS_FINDALL_REGEX_H_

#include <cstdint>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/array/qtype/types.h"        // IWYU pragma: keep
#include "arolla/dense_array/qtype/types.h"  // IWYU pragma: keep
#include "arolla/memory/buffer.h"
#include "arolla/qexpr/operators/strings/array_traits.h"
#include "arolla/qtype/base_types.h"  // IWYU pragma: keep
#include "arolla/qtype/strings/regex.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

// strings._findall_regex
struct FindallRegexOp {
  template <template <typename T> class ArrayType, typename StringType>
  absl::StatusOr<
      std::tuple<ArrayType<StringType>,
                 typename internal::ArrayTraits<ArrayType>::edge_type,
                 typename internal::ArrayTraits<ArrayType>::edge_type>>
  operator()(const ArrayType<StringType>& array, const RegexPtr& regex) const {
    using ArrayTraits = typename internal::ArrayTraits<ArrayType>;
    using EdgeType = ArrayTraits::edge_type;
    std::vector<absl::string_view> flattened_all_matches;
    std::vector<int64_t> match_split_points{0};
    match_split_points.reserve(array.size() + 1);
    array.ForEach([&](int64_t id, bool present, absl::string_view value) {
      match_split_points.push_back(match_split_points.back());
      if (present) {
        regex->FindAll(value, [&](absl::Span<const absl::string_view> groups) {
          match_split_points.back() += 1;
          flattened_all_matches.insert(flattened_all_matches.end(),
                                       groups.begin(), groups.end());
        });
      }
    });
    typename Buffer<StringType>::Builder builder(flattened_all_matches.size());
    typename Buffer<StringType>::Inserter inserter = builder.GetInserter();
    for (const auto& match_group_value : flattened_all_matches) {
      inserter.Add(match_group_value);
    }
    ASSIGN_OR_RETURN(
        auto matches_per_value_edge,
        EdgeType::FromSplitPoints(
            ArrayTraits::template CreateFromBuffer<int64_t>(
                Buffer<int64_t>::Create(std::move(match_split_points)))));
    ASSIGN_OR_RETURN(auto groups_per_match_edge,
                     EdgeType::FromUniformGroups(
                         /*parent_size=*/matches_per_value_edge.child_size(),
                         /*group_size=*/regex->NumberOfCapturingGroups()));
    return std::make_tuple(ArrayTraits::template CreateFromBuffer<StringType>(
                               std::move(builder).Build(inserter)),
                           std::move(matches_per_value_edge),
                           std::move(groups_per_match_edge));
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATORS_STRINGS_FINDALL_REGEX_H_
