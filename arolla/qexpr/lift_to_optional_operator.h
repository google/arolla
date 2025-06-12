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
#ifndef AROLLA_QEXPR_LIFT_TO_OPTIONAL_OPERATOR_H_
#define AROLLA_QEXPR_LIFT_TO_OPTIONAL_OPERATOR_H_

#include <type_traits>

#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/lifting.h"
#include "arolla/util/meta.h"

namespace arolla {

// Implementation of an operator on OptionalValue lifted from scalar operator
// (for OperatorFactory and simple_operator build rule). Use it via the
// lift_to_optional function in register_operator.bzl.
template <typename Op, typename ArgList>
class OptionalLiftedOperator;

template <typename Op, typename... Args>
class OptionalLiftedOperator<Op, meta::type_list<Args...>> {
  template <class T>
  using LiftedType =
      std::conditional_t<meta::is_wrapped_with_v<DoNotLiftTag, T>,
                         meta::strip_template_t<DoNotLiftTag, T>,
                         wrap_with_optional_t<T>>;

  // Lifted arguments are expected to be passed directly
  // in order to perform correct type deduction.
  template <class T>
  using LiftedTypeView = const T&;

 public:
  // Create an operation that capture all arguments marked with `DoNotLiftTag`
  // and accept other arguments as `OptionalValue` in the same order.
  // Note that `args` for lifted arguments are ignored and can be anything.
  // E.g., ArrayPointwiseLifter is calling this function with `Array`s.
  template <class... Ts>
  auto CreateOptionalOpWithCapturedScalars(const Ts&... args) const {
    using Tools = LiftingTools<Args...>;
    return WrapFnToAcceptOptionalArgs(
        Tools::template CreateFnWithDontLiftCaptured<LiftedTypeView>(Op(),
                                                                     args...));
  }

  auto operator()(const LiftedType<Args>&... args) const {
    using Tools = LiftingTools<Args...>;
    return Tools::CallOnLiftedArgs(CreateOptionalOpWithCapturedScalars(args...),
                                   args...);
  }
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_LIFT_TO_OPTIONAL_OPERATOR_H_
