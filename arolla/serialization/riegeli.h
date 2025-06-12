// Copyright 2025 Google LLC
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
#ifndef AROLLA_SERIALIZATION_RIEGELI_H_
#define AROLLA_SERIALIZATION_RIEGELI_H_

#include <string>

#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"

namespace arolla::serialization {

absl::StatusOr<std::string> EncodeAsRiegeliData(
    absl::Span<const TypedValue> values,
    absl::Span<const arolla::expr::ExprNodePtr> exprs,
    absl::string_view riegeli_options = "");

absl::StatusOr<DecodeResult> DecodeFromRiegeliData(
    absl::string_view riegeli_data,
    const DecodingOptions& decoding_options = {});

}  // namespace arolla::serialization

#endif  // AROLLA_SERIALIZATION_RIEGELI_H_
