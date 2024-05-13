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
#ifndef AROLLA_SERIALIZATION_UTILS_H_
#define AROLLA_SERIALIZATION_UTILS_H_

#include <string>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization_base/base.pb.h"

namespace arolla::serialization {

// Decodes an expression from the container, returns an error if there is not
// just one expression.
absl::StatusOr<arolla::expr::ExprNodePtr> DecodeExpr(
    const arolla::serialization_base::ContainerProto& container_proto,
    const DecodingOptions& options = {});

// Decodes a value from the container, returns an error if there is not
// just one value.
absl::StatusOr<TypedValue> DecodeValue(
    const arolla::serialization_base::ContainerProto& container_proto,
    const DecodingOptions& options = {});

// Decodes a set of named expressions from the container, which must contain
// expressions aligned with text values (for names).
absl::StatusOr<absl::flat_hash_map<std::string, arolla::expr::ExprNodePtr>>
DecodeExprSet(const arolla::serialization_base::ContainerProto& container_proto,
              const DecodingOptions& options = {});

// Encodes a set of named expressions. The names must be valid utf8 strings.
absl::StatusOr<arolla::serialization_base::ContainerProto> EncodeExprSet(
    const absl::flat_hash_map<std::string, arolla::expr::ExprNodePtr>&
        expr_set);

}  // namespace arolla::serialization

#endif  // AROLLA_SERIALIZATION_UTILS_H_
