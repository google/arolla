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
#ifndef AROLLA_EXPR_OPERATORS_CASTING_REGISTRY_H_
#define AROLLA_EXPR_OPERATORS_CASTING_REGISTRY_H_

#include <optional>

#include "absl/container/flat_hash_map.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/qtype/qtype.h"

namespace arolla::expr_operators {

// A registry of rules available for implicit casting.
class CastingRegistry {
  struct PrivateConstructorTag {};

 public:
  static CastingRegistry* GetInstance();

  CastingRegistry(PrivateConstructorTag);

  // Returns a list of operators that needs to be sequentially applied to
  // perform casting from from_qtype to to_qtype, or an error if no such casting
  // exists. If shape_for_broadcasting is provided, GetCast also supports
  // scalar broadcasting, taking the provided shape as a template.
  // Clients must call InitCore() before calling this method.
  absl::StatusOr<expr::ExprNodePtr> GetCast(
      expr::ExprNodePtr node, QTypePtr to_qtype, bool implicit_only,
      std::optional<expr::ExprNodePtr> shape_for_broadcasting =
          std::nullopt) const;

  // Returns the common type that all arg_types can be implicitly converted to,
  // or an error if the result is ambiguous or could not be calculated.
  absl::StatusOr<QTypePtr> CommonType(absl::Span<const QTypePtr> arg_types,
                                      bool enable_broadcasting = false) const;

 private:
  absl::flat_hash_map<QTypePtr, expr::ExprOperatorPtr> cast_to_ops_;
};

}  // namespace arolla::expr_operators

#endif  // AROLLA_EXPR_OPERATORS_CASTING_REGISTRY_H_
