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
#ifndef AROLLA_SERVING_EMBEDDED_MODEL_H_
#define AROLLA_SERVING_EMBEDDED_MODEL_H_

#include <functional>   // IWYU pragma: keep, macro definition
#include <type_traits>  // IWYU pragma: keep, macro definition

#include "absl/status/status.h"  // IWYU pragma: keep, macro definition
#include "absl/status/statusor.h"  // IWYU pragma: keep, macro definition
#include "absl/strings/str_format.h"  // IWYU pragma: keep, macro definition
#include "absl/strings/string_view.h"  // IWYU pragma: keep, macro definition
#include "arolla/util/indestructible.h"  // IWYU pragma: keep, macro definition
#include "arolla/util/init_arolla.h"  // IWYU pragma: keep, macro definition
#include "arolla/util/meta.h"  // IWYU pragma: keep, macro definition
#include "arolla/util/status_macros_backport.h"

// Defines a function to initialize and access a model embedded into the binary.
//
// Use it to simplify work with codegen models or embedded dynamic eval models.
// The provided model initialization code runs during InitArolla and the
// result is stored in a static variable. In addition, the macro validates that
// the model was successfully initialized during InitArolla.
//
// Usage example:
//
//   AROLLA_DEFINE_EMBEDDED_MODEL_FN(
//     MyModel,
//     (::arolla::ExprCompiler<...>().SetInputLoader(...).Compile(...)));
//
// defines function with semantic similar to
//
//   auto MyModel() {
//     static auto model = ::arolla::ExprCompiler<...>(
//          ).SetInputLoader(...).Compile(...).value();
//     return model;
//   }
//
// so it can be used like this
//
//   ASSIGN_OR_RETURN(auto evaluation_result, MyModel()(my_input));
//
// The macro must be used in *.cc file. If needed the function may be
// declared in a header file as:
//
//   using ModelFunction = ::arolla::ExprCompiler<...>::Function;
//   const ModelFunction& MyModel();
//
#define AROLLA_DEFINE_EMBEDDED_MODEL_FN(fn_name, model_or)                    \
  namespace {                                                                 \
  const decltype(model_or)& _arolla_embed_model_or_status_##fn_name() {       \
    using ModelT = decltype(model_or);                                        \
    static const ::arolla::Indestructible<ModelT> model(model_or);            \
    return *model;                                                            \
  }                                                                           \
  }                                                                           \
                                                                              \
  const ::arolla::meta::strip_template_t<absl::StatusOr, decltype(model_or)>& \
  fn_name() {                                                                 \
    const auto& model = _arolla_embed_model_or_status_##fn_name();            \
    /* Note that the "if" below will only be true if InitArolla is not        \
     * called or failed. */                                                   \
    if (!model.ok()) {                                                        \
      static ::arolla::meta::strip_template_t<absl::StatusOr,                 \
                                              decltype(model_or)>             \
          error_fn =                                                          \
              [status_(model.status())](const auto&...) { return status_; };  \
      return error_fn;                                                        \
    }                                                                         \
    return *model;                                                            \
  }                                                                           \
                                                                              \
  AROLLA_INITIALIZER(                                                         \
          .deps =                                                             \
              {                                                               \
                  "@phony/serving_compiler_optimizer",                        \
                  ::arolla::initializer_dep::kOperators,                      \
                  ::arolla::initializer_dep::kS11n,                           \
              },                                                              \
          .init_fn = []() -> absl::Status {                                   \
            RETURN_IF_ERROR(                                                  \
                _arolla_embed_model_or_status_##fn_name().status())           \
                << "while initializing embedded model " << #fn_name << " at " \
                << __FILE__ << ":" << __LINE__;                               \
            return absl::OkStatus();                                          \
          })

// Defines a function to initialize and access a model set embedded into the
// binary.
//
// Use it to simplify work with codegen models or embedded dynamic eval models.
// The provided model set initialization code runs during InitArolla and the
// result is stored in a static variable. In addition, the macro validates that
// all the models were successfully initialized during InitArolla.
//
// Usage example:
//
//   AROLLA_DEFINE_EMBEDDED_MODEL_SET_FN(
//      MyModelSet,
//      CompileExprSet(
//        ExprCompiler<...>().SetInputLoader(...),
//        GetMyModelSet()));
//
// defines MyModelSet(absl::string_view) function that can be used as
//
//   // NOTE that "auto" here is deduced to std::reference_wrapper. Use
//   // "const ModelFunction&" instead of "auto" if you want a real reference.
//   ASSIGN_OR_RETURN(auto my_model, MyModelSet("my_model"));
//   ASSIGN_OR_RETURN(auto evaluation_result, my_model(my_input));
//
// The macro must be used in *.cc file. If needed the function may be
// declared in a header file as:
//
//   using ModelFunction = ::arolla::ExprCompiler<...>::Function;
//   absl::StatusOr<std::reference_wrapper<const ModelFunction>>
//   MyModel(absl::string_view);
//
#define AROLLA_DEFINE_EMBEDDED_MODEL_SET_FN(fn_name, model_set_or)             \
  namespace {                                                                  \
  const decltype(model_set_or)&                                                \
      _arolla_embed_model_set_or_status_##fn_name() {                          \
    using ModelSetT = decltype(model_set_or);                                  \
    static const ::arolla::Indestructible<ModelSetT> model_set(model_set_or);  \
    return *model_set;                                                         \
  }                                                                            \
  }                                                                            \
                                                                               \
  absl::StatusOr<std::reference_wrapper<                                       \
      const std::decay_t<decltype(model_set_or->at(""))>>>                     \
  fn_name(absl::string_view model_name) {                                      \
    const auto& model_set = _arolla_embed_model_set_or_status_##fn_name();     \
    RETURN_IF_ERROR(model_set.status());                                       \
    auto it = model_set->find(model_name);                                     \
    if (it == model_set->end()) {                                              \
      return absl::NotFoundError(                                              \
          absl::StrFormat("model \"%s\" not found in " #fn_name, model_name)); \
    }                                                                          \
    return it->second;                                                         \
  }                                                                            \
                                                                               \
  AROLLA_INITIALIZER(                                                          \
          .deps =                                                              \
              {                                                                \
                  "@phony/serving_compiler_optimizer",                         \
                  ::arolla::initializer_dep::kOperators,                       \
                  ::arolla::initializer_dep::kS11n,                            \
              },                                                               \
          .init_fn = []() -> absl::Status {                                    \
            RETURN_IF_ERROR(                                                   \
                _arolla_embed_model_set_or_status_##fn_name().status())        \
                << "while initializing embedded model " << #fn_name << " at "  \
                << __FILE__ << ":" << __LINE__;                                \
            return absl::OkStatus();                                           \
          })

#endif  // AROLLA_SERVING_EMBEDDED_MODEL_H_
