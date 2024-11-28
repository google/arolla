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
#include "arolla/qtype/named_field_qtype.h"

#include <cstdint>
#include <optional>
#include <string>

#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "absl//strings/str_format.h"
#include "absl//strings/string_view.h"
#include "absl//types/span.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_ref.h"

namespace arolla {

absl::Span<const std::string> GetFieldNames(const QType* /*nullable*/ qtype) {
  const auto* named_field_qtype =
      dynamic_cast<const NamedFieldQTypeInterface*>(qtype);
  if (named_field_qtype == nullptr) {
    return {};
  }
  return named_field_qtype->GetFieldNames();
}

std::optional<int64_t> GetFieldIndexByName(const QType* /*nullable*/ qtype,
                                           absl::string_view field_name) {
  const auto* named_field_qtype =
      dynamic_cast<const NamedFieldQTypeInterface*>(qtype);
  if (named_field_qtype == nullptr) {
    return std::nullopt;
  }
  return named_field_qtype->GetFieldIndexByName(field_name);
}

absl::StatusOr<TypedRef> GetFieldByName(TypedRef qvalue,
                                        absl::string_view field_name) {
  std::optional<int64_t> idx =
      GetFieldIndexByName(qvalue.GetType(), field_name);
  if (!idx.has_value()) {
    return absl::InvalidArgumentError(absl::StrFormat(
        "no field named \"%s\" in %s", field_name, qvalue.GetType()->name()));
  }
  return qvalue.GetField(*idx);
}

const QType* /*nullable*/ GetFieldQTypeByName(const QType* /*nullable*/ qtype,
                                              absl::string_view field_name) {
  if (std::optional<int64_t> id = GetFieldIndexByName(qtype, field_name);
      id.has_value()) {
    return qtype->type_fields()[*id].GetType();
  }
  return nullptr;
}

}  // namespace arolla
