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
#ifndef AROLLA_OPTOOLS_OPTOOLS_H_
#define AROLLA_OPTOOLS_OPTOOLS_H_

#include <cstddef>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_operator_signature.h"
#include "arolla/qexpr/operator_factory.h"
#include "arolla/qexpr/operators.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::optools {

namespace optools_impl {

absl::Status RegisterFunctionAsOperatorImpl(
    absl::string_view name, std::vector<OperatorPtr> qexpr_ops,
    expr::ExprOperatorSignature signature, absl::string_view description);

// A helper that creates QExpr operators from a function or a tuple of functions
// using OperatorFactory::BuildFromFunction.
template <typename Fn>
struct MakeQExprOps {
  absl::StatusOr<std::vector<OperatorPtr>> operator()(Fn fn) {
    ASSIGN_OR_RETURN(OperatorPtr op, QExprOperatorFromFunction(std::move(fn)));
    return std::vector<OperatorPtr>{op};
  }
};

template <typename... FNs>
struct MakeQExprOps<std::tuple<FNs...>> {
  absl::StatusOr<std::vector<OperatorPtr>> operator()(std::tuple<FNs...> fns) {
    return impl(std::move(fns), std::index_sequence_for<FNs...>{});
  }

  template <size_t... Is>
  absl::StatusOr<std::vector<OperatorPtr>> impl(std::tuple<FNs...> fns,
                                                std::index_sequence<Is...>) {
    std::vector<OperatorPtr> res;
    res.reserve(sizeof...(FNs));
    for (auto status_or_op :
         {QExprOperatorFromFunction(std::move(std::get<Is>(fns)))...}) {
      RETURN_IF_ERROR(status_or_op.status());
      res.push_back(*status_or_op);
    }
    return res;
  }
};

}  // namespace optools_impl

// Registers a function as an operator. It constructs both qexpr and expr
// operators at the same time. Doesn't support functions with overloads, instead
// it is possible to pass a tuple of functions.
//
//     Usage example:
//
// int Add(int a, int b) { return a + b; }
// template <typename T> T Mul(T a, T b) { return a * b; }
//
// AROLLA_INITIALIZER(
//     .reverse_deps = {arolla::initializer_dep::kOperators},
//     .init_fn = []() -> absl::Status {
//       RETURN_IF_ERROR(optools::RegisterFunctionAsOperator(
//           Add, "optools_examle.add",
//           expr::ExprOperatorSignature::Make("a, b"), "Sum A and B"));
//       RETURN_IF_ERROR(optools::RegisterFunctionAsOperator(
//           std::make_tuple(Mul<float>, Mul<int>), "optools_examle.mul",
//           expr::ExprOperatorSignature::Make("a, b"),
//           "Multiply A and B"));
//       return absl::OkStatus();
//     })
//
//     The new operator can now be used in expressions:
// auto expr1 = expr::CallOp("optools_example.add", {a, b});
// auto expr2 = expr::CallOp("optools_example.mul", {a, b});
//
template <typename Fn>
absl::Status RegisterFunctionAsOperator(
    Fn&& fns, absl::string_view name,
    absl::StatusOr<expr::ExprOperatorSignature> signature =
        expr::ExprOperatorSignature(),
    absl::string_view doc = "") {
  RETURN_IF_ERROR(signature.status());
  ASSIGN_OR_RETURN(std::vector<OperatorPtr> ops,
                   optools_impl::MakeQExprOps<Fn>()(fns));
  return optools_impl::RegisterFunctionAsOperatorImpl(
      name, std::move(ops), *std::move(signature), doc);
}

}  // namespace arolla::optools

#endif  // AROLLA_OPTOOLS_OPTOOLS_H_
