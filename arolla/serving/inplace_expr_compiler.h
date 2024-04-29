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
#ifndef AROLLA_SERVING_INPLACE_EXPR_COMPILER_H_
#define AROLLA_SERVING_INPLACE_EXPR_COMPILER_H_

#include <functional>
#include <memory>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "arolla/expr/expr_node.h"
#include "arolla/io/input_loader.h"
#include "arolla/io/slot_listener.h"
#include "arolla/io/struct_io.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla {

namespace inplace_expr_compiler_impl {

using TypedSlotMap = absl::flat_hash_map<std::string, TypedSlot>;

TypedSlotMap CollectInternalSlots(TypedSlot root_slot);

struct IoSlots {
  TypedSlotMap input_slots;
  TypedSlot output_slot;
  TypedSlotMap named_output_slots;
};

// Returns slots required for binding expression.
// Names are created using naming/table.h library.
absl::StatusOr<IoSlots> CollectIoSlots(QTypePtr qtype,
                                       const CompiledExpr& compiled_expr,
                                       absl::string_view final_output_name);

}  // namespace inplace_expr_compiler_impl

// For working with inplace evaluation one need to define type STRUCT_TYPE
// that satisfies the following requirements:
// 1. satisfy StandardLayoutType (e.g., POD).
//    https://en.cppreference.com/w/cpp/named_req/StandardLayoutType
// 2. registered as `QType` with subfields using `ArollaStructFields`.
//    See util/struct_field.h.
//
// Example of type that can be used:
//
// In header file:
// struct TestOutputStruct {
//   double x_plus_y;
//   double x_times_y;

//   constexpr static auto ArollaStructFields() {
//     using CppType = TestOutputStruct;
//     return std::tuple{
//         AROLLA_DECLARE_STRUCT_FIELD(x_plus_y),
//         AROLLA_DECLARE_STRUCT_FIELD(x_times_y),
//     };
//   }
// };

// struct TestStruct {
//   float x;
//   double y;
//   TestOutputStruct side_outputs;

//   constexpr static auto ArollaStructFields() {
//     using CppType = TestStruct;
//     return std::tuple{
//         AROLLA_DECLARE_STRUCT_FIELD(x),
//         AROLLA_DECLARE_STRUCT_FIELD(y),
//         AROLLA_DECLARE_STRUCT_FIELD(side_outputs),
//     };
//   }
// };
// namespace arolla {
// AROLLA_DECLARE_SIMPLE_QTYPE(TEST_OUTPUT_STRUCT, TestOutputStruct);
// AROLLA_DEFINE_SIMPLE_QTYPE(TEST_STRUCT, TestStruct);
// }  // namespace arolla
//
// In cc file:
// namespace arolla {
// AROLLA_DEFINE_SIMPLE_QTYPE(TEST_OUTPUT_STRUCT, TestOutputStruct);
// AROLLA_DEFINE_SIMPLE_QTYPE(TEST_STRUCT, TestStruct);
// }  // namespace arolla
//

// Function evaluating model on value and writing result inside of it.
template <class T>
using InplaceModelFunction = std::function<absl::Status(T&)>;

// Compile expression for evaluation on type T inplace.
// Following requirements must be satisfied:
// 0. T must be STRUCT_TYPE
// 1. `compiled_expr` must be codegenerated model
//    (doesn't use intermediate slots)
// 2. `compiled_expr.input_types()` must match field types exactly.
// 3. `compiled_expr.output_type()` must correspond exactly to
//    type of the field `final_output_name`.
// 4. compiled_expr.named_output_types() must must match field types exactly.
// Note for 2-4:
//    Field names are created using `::arolla::naming` library based on
//    ArollaStructFields.
//    E.g., TablePath().Child("side_outputs").Column("x_plus_y")
template <typename T>
absl::StatusOr<InplaceModelFunction<T>> CompileInplaceExprOnStruct(
    const InplaceCompiledExpr& compiled_expr,
    absl::string_view final_output_name) {
  static_assert(
      std::is_standard_layout<T>::value,
      "Data must be standard layout to be used with CompileExprInplace.");
  QTypePtr qtype = GetQType<T>();
  ASSIGN_OR_RETURN(inplace_expr_compiler_impl::IoSlots slots,
                   inplace_expr_compiler_impl::CollectIoSlots(
                       qtype, compiled_expr, final_output_name));
  ASSIGN_OR_RETURN(auto executable, compiled_expr.InplaceBind(
                                        slots.input_slots, slots.output_slot,
                                        slots.named_output_slots));
  return [qtype, executable(std::shared_ptr<BoundExpr>(std::move(executable)))](
             T& input) -> absl::Status {
    FramePtr frame(&input, &qtype->type_layout());
    EvaluationContext ctx;
    executable->Execute(&ctx, frame);
    return ctx.status();
  };
}

// With arolla annotated struct it is also possible to evaluate dynamic
// expression or expression with outputs not storable in the struct
// (e.g., Tuple/TypedValue).
//
// For that regular `ExprCompiler` together with `CreateStructInputLoader`
// and `CreateStructSlotListener` can be used.
//
// Example:
// ASSIGN_OR_RETURN(  // Should be done once, not on every evaluation.
//     std::function<absl::StatusOr<double>(const TestStruct&)> eval_fn,
//     (arolla::ExprCompiler<TestStruct, double>())
//       .SetInputLoader(arolla::CreateStructInputLoader<TestStruct>())
//       .Compile(expr)));
// TestStruct input{.x = 5.f, .y = 7.};
// ASSIGN_OR_RETURN(double result, eval_fn(input));
//
// Fields required for the computation will be copied to the evaluation context
// by offset within the struct (using arolla::StructInputLoader).
// Exported fields will be copied from the evaluation context
// into the output struct by offset (using arolla::StructSlotListener).
//
// Field names in the struct are created using
// `::arolla::naming` library based on ArollaStructFields.
// E.g., TablePath().Child("side_outputs").Column("x_plus_y")
//
// If leaf nodes are annotated with `annotation.qtype`.
// `QType`s must match field types exactly.
//
// Example with storing side outputs (to the same struct in this case):
// ASSIGN_OR_RETURN(  // Should be done once, not on every evaluation.
//     std::function<absl::StatusOr<double>(const TestStruct&, TestStruct*)>
//         eval_fn,
//     (arolla::ExprCompiler<TestStruct, double, TestStruct>())
//       .SetInputLoader(arolla::CreateStructInputLoader<TestStruct>())
//       .SetSlotListener(arolla::CreateStructSlotListener<TestStruct>())
//       .Compile(expr)));
// TestStruct data{.x = 5.f, .y = 7.};
// ASSIGN_OR_RETURN(double result, eval_fn(data, &data));
// double x_times_y = data.side_outputs.x_times_y;
//
// For CompiledExpr prefer CompileInplaceExprOnStruct to achieve
// the best performance.
// But the same method is useful iff storing output or side outputs inside of
// the struct is not possible. E.g., for Tuple/TypedValue output.

// Creates InputLoader for given struct with defined QType with named fields.
template <typename Struct>
absl::StatusOr<InputLoaderPtr<Struct>> CreateStructInputLoader() {
  return StructInputLoader<Struct>::Create(
      inplace_expr_compiler_impl::CollectInternalSlots(
          TypedSlot::UnsafeFromOffset(GetQType<Struct>(), 0)));
}

// Creates SlotListener for given struct with defined QType with named fields.
template <typename Struct>
absl::StatusOr<std::unique_ptr<SlotListener<Struct>>>
CreateStructSlotListener() {
  return StructSlotListener<Struct>::Create(
      inplace_expr_compiler_impl::CollectInternalSlots(
          TypedSlot::UnsafeFromOffset(GetQType<Struct>(), 0)));
}

}  // namespace arolla

#endif  // AROLLA_SERVING_INPLACE_EXPR_COMPILER_H_
