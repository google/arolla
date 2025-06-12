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
#ifndef AROLLA_EXPR_OPERATOR_LOADER_GENERIC_OPERATOR_OVERLOAD_CONDITION_H_
#define AROLLA_EXPR_OPERATOR_LOADER_GENERIC_OPERATOR_OVERLOAD_CONDITION_H_

#include <vector>

#include "absl/functional/any_invocable.h"
#include "absl/status/statusor.h"
#include "absl/types/span.h"
#include "arolla/expr/expr_node.h"
#include "arolla/qtype/qtype.h"

namespace arolla::operator_loader {

// Compiled GenericOperator overload conditions.
//
// The L.input_tuple_qtype shall be a tuple qtype with the fields corresponding
// to the node dependencies, with NOTHING for to unavailable qtypes.
using GenericOperatorOverloadConditionFn =
    absl::AnyInvocable<absl::StatusOr<std::vector<bool>>(
        QTypePtr input_tuple_qtype) const>;

// Returns std::function<> that evaluates the given overload conditions.
//
// Each overload condition shall depend only on the leaf L.input_tuple_qtype and
// return an OPTIONAL_UNIT value.
absl::StatusOr<GenericOperatorOverloadConditionFn>
MakeGenericOperatorOverloadConditionFn(
    absl::Span<const ::arolla::expr::ExprNodePtr> prepared_condition_exprs);

}  // namespace arolla::operator_loader

#endif  // AROLLA_EXPR_OPERATOR_LOADER_GENERIC_OPERATOR_OVERLOAD_CONDITION_H_
