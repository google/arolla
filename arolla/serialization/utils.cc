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
#include "arolla/serialization/utils.h"

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/base_types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::serialization {

using ::arolla::expr::ExprNodePtr;
using ::arolla::serialization_base::ContainerProto;

absl::StatusOr<ExprNodePtr> DecodeExpr(const ContainerProto& container_proto,
                                       const DecodingOptions& options) {
  ASSIGN_OR_RETURN(auto decode_result, Decode(container_proto, options));
  if (decode_result.exprs.size() != 1 || !decode_result.values.empty()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unable to decode expression: expected 1 expression and 0 values in "
        "the container, got %d and %d",
        decode_result.exprs.size(), decode_result.values.size()));
  }
  return decode_result.exprs[0];
}

absl::StatusOr<TypedValue> DecodeValue(const ContainerProto& container_proto,
                                       const DecodingOptions& options) {
  ASSIGN_OR_RETURN(auto decode_result, Decode(container_proto, options));
  if (decode_result.values.size() != 1 || !decode_result.exprs.empty()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "unable to decode value: expected 1 value and 0 expressions in "
        "the container, got %d and %d",
        decode_result.values.size(), decode_result.exprs.size()));
  }
  return decode_result.values[0];
}

absl::StatusOr<absl::flat_hash_map<std::string, ExprNodePtr>> DecodeExprSet(
    const arolla::serialization_base::ContainerProto& container_proto,
    const DecodingOptions& options) {
  ASSIGN_OR_RETURN(auto decode_result, Decode(container_proto, options));
  if (decode_result.values.size() != decode_result.exprs.size()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "the number of expressions does not match the number of values:"
        " %d != %d",
        decode_result.exprs.size(), decode_result.values.size()));
  }
  absl::flat_hash_map<std::string, ExprNodePtr> result;
  result.reserve(decode_result.exprs.size());
  for (size_t i = 0; i < decode_result.exprs.size(); ++i) {
    if (decode_result.values[i].GetType() != GetQType<Text>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected all values in the container to be TEXTs, got %s",
          decode_result.values[i].GetType()->name()));
    }
    const auto name = decode_result.values[i].UnsafeAs<Text>().view();
    if (!result.emplace(name, decode_result.exprs[i]).second) {
      return absl::InvalidArgumentError(
          absl::StrFormat("duplicated names in the container: \"%s\"",
                          absl::Utf8SafeCHexEscape(name)));
    }
  }
  return result;
}

absl::StatusOr<ContainerProto> EncodeExprSet(
    const absl::flat_hash_map<std::string, ExprNodePtr>& expr_set) {
  std::vector<absl::string_view> names;
  names.reserve(expr_set.size());
  for (const auto& [name, _] : expr_set) {
    names.emplace_back(name);
  }
  std::sort(names.begin(), names.end());  // Make the order deterministic.
  std::vector<TypedValue> values;
  std::vector<ExprNodePtr> exprs;
  values.reserve(names.size());
  exprs.reserve(names.size());
  for (size_t i = 0; i < names.size(); ++i) {
    values.push_back(TypedValue::FromValue(Text(names[i])));
    exprs.push_back(expr_set.at(names[i]));
  }
  return Encode(values, exprs);
}

}  // namespace arolla::serialization
