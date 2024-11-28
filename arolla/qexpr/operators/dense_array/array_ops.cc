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
#include "arolla/qexpr/operators/dense_array/array_ops.h"

#include <cstdint>

#include "absl//status/status.h"
#include "absl//strings/str_format.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/qexpr/eval_context.h"

namespace arolla {

void DenseArrayAtOp::ReportIndexOutOfRangeError(EvaluationContext* ctx,
                                                int64_t index, int64_t size) {
  if (ctx->status().ok()) {
    ctx->set_status(absl::InvalidArgumentError(
        absl::StrFormat("array index %d out of range [0, %d)", index, size)));
  }
}

bool DenseArrayFromIndicesAndValues::ValidateInputs(
    EvaluationContext* ctx, const DenseArray<int64_t>& indices,
    int64_t values_size, int64_t size) {
  if (indices.size() != values_size) {
    ctx->set_status(absl::InvalidArgumentError(
        absl::StrFormat("expected arrays of the same sizes, got "
                        "indices.size=%d, values.size=%d",
                        indices.size(), values_size)));
    return false;
  }
  if (size < 0) {
    ctx->set_status(absl::InvalidArgumentError(
        absl::StrFormat("expected a non-negative integer, got size=%d", size)));
    return false;
  }
  if (!indices.IsFull()) {
    ctx->set_status(
        absl::InvalidArgumentError("missing indices are not supported"));
    return false;
  }
  int64_t last_index = -1;
  for (int64_t index : indices.values) {
    if (index <= last_index) {
      if (index < 0) {
        ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
            "expected non-negative indices, got index=%d", index)));
        return false;
      } else {
        ctx->set_status(absl::InvalidArgumentError(
            absl::StrFormat("expected a strictly increasing sequence of "
                            "indices, got [..., %d, %d, ...]",
                            last_index, index)));
        return false;
      }
    } else if (index >= size) {
      ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
          "index is out of range, index=%d >= size=%d", index, size)));
      return false;
    }
    last_index = index;
  }
  return true;
}

}  // namespace arolla
