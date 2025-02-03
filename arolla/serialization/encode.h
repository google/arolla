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
#ifndef AROLLA_SERIALIZATION_ENCODE_H_
#define AROLLA_SERIALIZATION_ENCODE_H_

#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization_base/base.pb.h"

namespace arolla::serialization {

// Encodes the given values and expressions using all known codecs.
absl::StatusOr<arolla::serialization_base::ContainerProto> Encode(
    absl::Span<const TypedValue> values,
    absl::Span<const expr::ExprNodePtr> exprs);

}  // namespace arolla::serialization

#endif  // AROLLA_SERIALIZATION_ENCODE_H_
