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
#ifndef AROLLA_QEXPR_EVALUATION_ENGINE_H_
#define AROLLA_QEXPR_EVALUATION_ENGINE_H_

#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "absl//container/flat_hash_map.h"
#include "absl//status/status.h"
#include "absl//status/statusor.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {

// Interfaces for evaluation of the expressions.
//
// There are two classes involved in this low level evaluation.
// 2. CompiledExpr
// 3. BoundExpr
//
// Usage example:
// FrameLayout::Builder layout_builder;
// ASSERT_OK_AND_ASSIGN(auto compiled_expr, Compile(expr));
// ASSERT_OK_AND_ASSIGN(
//     auto executable_expr,
//     compiled_expr->Bind(
//         &layout_builder,
//         AddSlotsMap(compiled_expr->input_types(), &layout_builder),
//         AddSlot(compiled_expr->output_type(), &layout_builder)));
// TypedSlot x = executable_expr->input_slots().at("x");
// TypedSlot y = executable_expr->output_slot();
// TypedSlot ax = executable_expr->named_output_slots().at("ax");
//
// FrameLayout layout = std::move(layout_builder).Build();
// RootEvaluationContext ctx(&layout);
// ctx.Set(x.ToSlot<float>().value(), 3.0f);
// CHECK_OK(executable_expr->InitializeLiterals(&ctx));
// CHECK_OK(executable_expr->Execute(&ctx));

// Expression bound to the specific slots.
class BoundExpr {
 public:
  virtual ~BoundExpr() = default;

  const absl::flat_hash_map<std::string, TypedSlot>& input_slots() const {
    return input_slots_;
  }
  TypedSlot output_slot() const { return output_slot_; }

  // Additionally tracked named side outputs.
  const absl::flat_hash_map<std::string, TypedSlot>& named_output_slots()
      const {
    return named_output_slots_;
  }

  // Load literal values from expression into the EvaluationContext. Since these
  // literal values are not changed during evaluation, it is possible to
  // reuse the EvaluationContext without reloading the literals.
  //
  // In case of an error, the method reports it through `ctx->status`.
  // It's caller's responsibility to check `ctx->status` before calling another
  // operation using the same `ctx`.
  virtual void InitializeLiterals(EvaluationContext*, FramePtr) const = 0;

  // Load literal values from expression into the EvaluationContext. Since these
  // literal values are not changed during evaluation, it is possible to
  // reuse the EvaluationContext without reloading the literals.
  absl::Status InitializeLiterals(RootEvaluationContext* root_ctx) const {
    EvaluationContext ctx(*root_ctx);
    InitializeLiterals(&ctx, root_ctx->frame());
    return std::move(ctx).status();
  }

  // Executes the expression. This function assumes that the provided
  // EvaluationContext's literals are initialized (see InitializeLiterals),
  // and input slots have been initialized.
  //
  // In case of an error, the method reports it through `ctx->status`.
  // It's caller's responsibility to check `ctx->status` before calling another
  // operation using the same `ctx`.
  virtual void Execute(EvaluationContext*, FramePtr) const = 0;

  // Executes the expression. This function assumes that the provided
  // EvaluationContext's literals are initialized (see InitializeLiterals),
  // and input slots have been initialized.
  absl::Status Execute(RootEvaluationContext* root_ctx) const {
    EvaluationContext ctx(*root_ctx);
    Execute(&ctx, root_ctx->frame());
    return std::move(ctx).status();
  }

 protected:
  BoundExpr(absl::flat_hash_map<std::string, TypedSlot> input_slots,
            TypedSlot output_slot,
            absl::flat_hash_map<std::string, TypedSlot> named_output_slots)
      : input_slots_(std::move(input_slots)),
        output_slot_(output_slot),
        named_output_slots_(std::move(named_output_slots)) {}

 private:
  const absl::flat_hash_map<std::string, TypedSlot> input_slots_;
  const TypedSlot output_slot_;
  const absl::flat_hash_map<std::string, TypedSlot> named_output_slots_;
};

// Expression compiled for a specific types and backend.
class CompiledExpr {
 public:
  CompiledExpr(absl::flat_hash_map<std::string, QTypePtr> input_types,
               QTypePtr output_type,
               absl::flat_hash_map<std::string, QTypePtr> named_output_types)
      : input_types_(std::move(input_types)),
        output_type_(std::move(output_type)),
        named_output_types_(std::move(named_output_types)) {}
  virtual ~CompiledExpr() = default;

  // Expected input types of the expression.
  const absl::flat_hash_map<std::string, QTypePtr>& input_types() const {
    return input_types_;
  }

  // Output types of the expression.
  QTypePtr output_type() const { return output_type_; }

  // Expected named output types of the expression.
  const absl::flat_hash_map<std::string, QTypePtr>& named_output_types() const {
    return named_output_types_;
  }

  // Binds expression to the specific slots in the layout.
  virtual absl::StatusOr<std::unique_ptr<BoundExpr>> Bind(
      // Used to define the layout of memory for evaluation of this expression.
      // Allows multiple expressions to share a single memory layout.
      FrameLayout::Builder* layout_builder,

      // Pre-allocated slots to be bound to expression inputs.
      // If an expression input is not found in this map, error is returned.
      // Error returned on type mismatch with input_types().
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,

      // Optional preallocated output slot.
      // Error returned on type mismatch with output_type().
      std::optional<TypedSlot> output_slot) const = 0;

  // Bind without IO slots. All slots will be created in the layout.
  absl::StatusOr<std::unique_ptr<BoundExpr>> Bind(
      FrameLayout::Builder* layout_builder) const {
    return Bind(layout_builder, AddSlotsMap(input_types(), layout_builder),
                AddSlot(output_type(), layout_builder));
  }

 private:
  absl::flat_hash_map<std::string, QTypePtr> input_types_;
  QTypePtr output_type_;
  absl::flat_hash_map<std::string, QTypePtr> named_output_types_;
};

// Expression compiled for a specific types and backend that is possible to
// evaluate without additional intermediate slots.
class InplaceCompiledExpr : public CompiledExpr {
 public:
  using CompiledExpr::CompiledExpr;

  // Implementation of base interface Bind via InplaceBind.
  absl::StatusOr<std::unique_ptr<BoundExpr>> Bind(
      FrameLayout::Builder* layout_builder,
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,
      std::optional<TypedSlot> output_slot) const final;

  // Binds expression to the specific slots in the layout.
  // No intermediate slots are allowed for the evaluation.
  // `BoundExpr::InitializeLiterals` in the returning executable
  // guaranteed to be no op.
  virtual absl::StatusOr<std::unique_ptr<BoundExpr>> InplaceBind(
      // Pre-allocated slots to be bound to expression inputs.
      // If an expression input is not found in this map, error is returned.
      // Error returned on type mismatch with input_types().
      const absl::flat_hash_map<std::string, TypedSlot>& input_slots,

      // Preallocated output slot.
      // Error returned on type mismatch with output_type().
      TypedSlot output_slot,

      // Preallocated named output slots.
      // Error returned on type mismatch with named_output_types().
      const absl::flat_hash_map<std::string, TypedSlot>& named_output_slots)
      const = 0;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_EVALUATION_ENGINE_H_
