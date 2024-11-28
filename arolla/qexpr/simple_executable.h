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
#ifndef AROLLA_QEXPR_SIMPLE_EXECUTABLE_H_
#define AROLLA_QEXPR_SIMPLE_EXECUTABLE_H_

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl//container/flat_hash_map.h"
#include "arolla/memory/frame.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/evaluation_engine.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qtype/typed_slot.h"

namespace arolla {

// A minimal executable expr implementation.
class SimpleBoundExpr : public BoundExpr {
 public:
  SimpleBoundExpr(
      absl::flat_hash_map<std::string, TypedSlot> input_slots,
      TypedSlot output_slot,
      std::vector<std::unique_ptr<BoundOperator>> init_ops,
      std::vector<std::unique_ptr<BoundOperator>> eval_ops,
      absl::flat_hash_map<std::string, TypedSlot> named_output_slots = {})
      : BoundExpr(std::move(input_slots), output_slot,
                  std::move(named_output_slots)),
        init_ops_(std::move(init_ops)),
        eval_ops_(std::move(eval_ops)) {}

  void InitializeLiterals(EvaluationContext* ctx,
                          FramePtr frame) const override;

  void Execute(EvaluationContext* ctx, FramePtr frame) const override;

 private:
  std::vector<std::unique_ptr<BoundOperator>> init_ops_;
  std::vector<std::unique_ptr<BoundOperator>> eval_ops_;
};

// BoundExpr evaluating all the provided subexpressions in order.
// input_slots, output_slot and named_output_slots must be configured
// consistently with the provided expressions.
class CombinedBoundExpr : public BoundExpr {
 public:
  CombinedBoundExpr(
      absl::flat_hash_map<std::string, TypedSlot> input_slots,
      TypedSlot output_slot,
      absl::flat_hash_map<std::string, TypedSlot> named_output_slots,
      std::vector<std::unique_ptr<BoundExpr>> subexprs)
      : BoundExpr(std::move(input_slots), output_slot,
                  std::move(named_output_slots)),
        subexprs_(std::move(subexprs)) {}

  void InitializeLiterals(EvaluationContext* ctx,
                          FramePtr frame) const override;

  void Execute(EvaluationContext* ctx, FramePtr frame) const override;

 private:
  std::vector<std::unique_ptr<BoundExpr>> subexprs_;
};

}  // namespace arolla

#endif  // AROLLA_QEXPR_SIMPLE_EXECUTABLE_H_
