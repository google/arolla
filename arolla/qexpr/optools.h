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
#ifndef AROLLA_QEXPR_OPTOOLS_H_
#define AROLLA_QEXPR_OPTOOLS_H_

#include <memory>  // IWYU pragma: keep

#include "arolla/qexpr/operator_factory.h"  // IWYU pragma: keep
#include "arolla/qexpr/operators.h"         // IWYU pragma: keep
#include "arolla/util/init_arolla.h"        // IWYU pragma: keep
#include "arolla/util/status_macros_backport.h"                     // IWYU pragma: keep

// Creates and registers a QExpr operator from a function.
//
// The operator input and output QTypes are inferred from the function. One can
// register several operators with the same name, but with different signatures.
//
// NOTE: this macro is a simpler alternative to operator_libraries BUILD rule.
// Unlike it, the macro does not register metadata for Codegen, so the operator
// will be available only in Dynamic Eval.
//
// The macro must only be used from a *.cc file in an alwayslink=1 cc_library.
//
// Example usages:
//
// AROLLA_REGISTER_QEXPR_OPERATOR("my_namespace.add", [](float a, float b) {
//       return a + b;
//     });
//
// double AddDoubles(double a, double b) { return a + b; }
// AROLLA_REGISTER_QEXPR_OPERATOR("my_namespace.add", AddDoubles);
//
// template <typename T>
// struct AddTs {
//   T operator()(const T& a, const T& b) const { return a + b; }
// };
// AROLLA_REGISTER_QEXPR_OPERATOR("my_namespace.add", AddTs<int64_t>());
//
#define AROLLA_REGISTER_QEXPR_OPERATOR(op_name, op_fn)                \
  AROLLA_INITIALIZER(                                                 \
          .reverse_deps =                                             \
              {                                                       \
                  ::arolla::initializer_dep::kOperators,              \
                  ::arolla::initializer_dep::kQExprOperators,         \
              },                                                      \
          .init_fn = []() -> absl::Status {                           \
            ASSIGN_OR_RETURN(auto op, ::arolla::OperatorFactory()     \
                                          .WithName(op_name)          \
                                          .BuildFromFunction(op_fn)); \
            return ::arolla::OperatorRegistry::GetInstance()          \
                ->RegisterOperator(std::move(op));                    \
          })

// Registers a QExpr OperatorFamily.
//
// NOTE: this macro is a simpler alternative to operator_family BUILD rule.
// Unlike it, the macro does not register metadata for Codegen, so the operator
// will be available only in Dynamic Eval.
//
// The macro must only be used from a *.cc file in an alwayslink=1 cc_library.
//
// Example usage:
//
// class AddFamily : public arolla::OperatorFamily {
//    // ...
// };
// AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY("my_namespace.add", AddFamily);
//
#define AROLLA_REGISTER_QEXPR_OPERATOR_FAMILY(op_name, op_family)        \
  AROLLA_INITIALIZER(                                                    \
          .reverse_deps =                                                \
              {                                                          \
                  ::arolla::initializer_dep::kOperators,                 \
                  ::arolla::initializer_dep::kQExprOperators,            \
              },                                                         \
          .init_fn = [] {                                                \
            return ::arolla::OperatorRegistry::GetInstance()             \
                ->RegisterOperatorFamily(op_name,                        \
                                         std::make_unique<op_family>()); \
          })

#endif  // AROLLA_QEXPR_OPTOOLS_H_
