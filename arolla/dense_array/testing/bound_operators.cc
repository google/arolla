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
#include "arolla/dense_array/testing/bound_operators.h"

#include <memory>

#include "absl/status/status.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/ops/dense_ops.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/optional_value.h"
#include "arolla/qexpr/eval_context.h"
#include "arolla/qexpr/operators.h"
#include "arolla/qexpr/operators/math/batch_arithmetic.h"

namespace arolla {
namespace {

// These operators are implemented in .cc rather than in header in order to
// prevent inlining into benchmark.

class TestAdd : public BoundOperator {
 public:
  explicit TestAdd(FrameLayout::Slot<DenseArray<float>> arg1,
                   FrameLayout::Slot<DenseArray<float>> arg2,
                   FrameLayout::Slot<DenseArray<float>> result)
      : arg1_(arg1), arg2_(arg2), result_(result) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    if (frame.Get(arg1_).size() != frame.Get(arg2_).size()) {
      ctx->set_status(absl::InvalidArgumentError("size mismatch"));
    } else {
      auto op = CreateDenseOp<DenseOpFlags::kRunOnMissing |
                              DenseOpFlags::kNoBitmapOffset |
                              DenseOpFlags::kNoSizeValidation>(
          [](float a, float b) { return a + b; }, &ctx->buffer_factory());
      *frame.GetMutable(result_) = op(frame.Get(arg1_), frame.Get(arg2_));
    }
  }

 private:
  FrameLayout::Slot<DenseArray<float>> arg1_;
  FrameLayout::Slot<DenseArray<float>> arg2_;
  FrameLayout::Slot<DenseArray<float>> result_;
};

class TestEigenAdd : public BoundOperator {
 public:
  explicit TestEigenAdd(FrameLayout::Slot<DenseArray<float>> arg1,
                        FrameLayout::Slot<DenseArray<float>> arg2,
                        FrameLayout::Slot<DenseArray<float>> result)
      : arg1_(arg1), arg2_(arg2), result_(result) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    if (frame.Get(arg1_).size() != frame.Get(arg2_).size()) {
      ctx->set_status(absl::InvalidArgumentError("size mismatch"));
    } else {
      auto op =
          CreateDenseBinaryOpFromSpanOp<float,
                                        DenseOpFlags::kNoBitmapOffset |
                                            DenseOpFlags::kNoSizeValidation>(
              BatchAdd<float>(), &ctx->buffer_factory());
      *frame.GetMutable(result_) = op(frame.Get(arg1_), frame.Get(arg2_));
    }
  }

 private:
  FrameLayout::Slot<DenseArray<float>> arg1_;
  FrameLayout::Slot<DenseArray<float>> arg2_;
  FrameLayout::Slot<DenseArray<float>> result_;
};

class TestUnionAdd : public BoundOperator {
 public:
  explicit TestUnionAdd(FrameLayout::Slot<DenseArray<float>> arg1,
                        FrameLayout::Slot<DenseArray<float>> arg2,
                        FrameLayout::Slot<DenseArray<float>> result)
      : arg1_(arg1), arg2_(arg2), result_(result) {}

  void Run(EvaluationContext* ctx, FramePtr frame) const override {
    if (frame.Get(arg1_).size() != frame.Get(arg2_).size()) {
      ctx->set_status(absl::InvalidArgumentError("size mismatch"));
    } else {
      auto fn = [](OptionalValue<float> a, OptionalValue<float> b) {
        OptionalValue<float> res;
        res.present = a.present || b.present;
        res.value = (a.present ? a.value : 0.f) + (b.present ? b.value : 0.f);
        return res;
      };
      auto op = CreateDenseOp<DenseOpFlags::kNoBitmapOffset |
                              DenseOpFlags::kNoSizeValidation>(
          fn, &ctx->buffer_factory());
      *frame.GetMutable(result_) = op(frame.Get(arg1_), frame.Get(arg2_));
    }
  }

 private:
  FrameLayout::Slot<DenseArray<float>> arg1_;
  FrameLayout::Slot<DenseArray<float>> arg2_;
  FrameLayout::Slot<DenseArray<float>> result_;
};

}  // namespace

std::unique_ptr<BoundOperator> DenseArrayAddOperator(
    FrameLayout::Slot<DenseArray<float>> arg1,
    FrameLayout::Slot<DenseArray<float>> arg2,
    FrameLayout::Slot<DenseArray<float>> result) {
  return std::make_unique<TestAdd>(arg1, arg2, result);
}

std::unique_ptr<BoundOperator> DenseArrayEigenAddOperator(
    FrameLayout::Slot<DenseArray<float>> arg1,
    FrameLayout::Slot<DenseArray<float>> arg2,
    FrameLayout::Slot<DenseArray<float>> result) {
  return std::make_unique<TestEigenAdd>(arg1, arg2, result);
}

std::unique_ptr<BoundOperator> DenseArrayUnionAddOperator(
    FrameLayout::Slot<DenseArray<float>> arg1,
    FrameLayout::Slot<DenseArray<float>> arg2,
    FrameLayout::Slot<DenseArray<float>> result) {
  return std::make_unique<TestUnionAdd>(arg1, arg2, result);
}

}  // namespace arolla
