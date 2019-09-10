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
#include "arolla/qexpr/operators/array/array_ops.h"

#include <cstdint>
#include <optional>
#include <utility>

#include "absl/status/status.h"
#include "absl/strings/str_format.h"
#include "arolla/array/array.h"
#include "arolla/array/id_filter.h"
#include "arolla/qexpr/eval_context.h"

namespace arolla {

void ArrayAtOp::ReportIndexOutOfRangeError(EvaluationContext* ctx,
                                           int64_t index, int64_t size) {
  if (ctx->status().ok()) {
    ctx->set_status(absl::InvalidArgumentError(
        absl::StrFormat("array index %d out of range [0, %d)", index, size)));
  }
}

std::optional<IdFilter> ArrayFromIndicesAndValues::ValidateInputs(
    EvaluationContext* ctx, const Array<int64_t>& indices, int64_t values_size,
    int64_t size) {
  if (indices.size() != values_size) {
    ctx->set_status(absl::InvalidArgumentError(
        absl::StrFormat("expected arrays of the same sizes, got "
                        "indices.size=%d, values.size=%d",
                        indices.size(), values_size)));
    return std::nullopt;
  }
  if (size < 0) {
    ctx->set_status(absl::InvalidArgumentError(
        absl::StrFormat("expected a non-negative integer, got size=%d", size)));
    return std::nullopt;
  }
  if (indices.PresentCount() != indices.size()) {
    ctx->set_status(
        absl::InvalidArgumentError("missing indices are not supported"));
    return std::nullopt;
  }
  const auto raw_indices = indices.ToDenseForm().dense_data().values;
  int64_t last_index = -1;
  for (int64_t index : raw_indices.span()) {
    if (index <= last_index) {
      if (index < 0) {
        ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
            "expected non-negative indices, got index=%d", index)));
        return std::nullopt;
      } else {
        ctx->set_status(absl::InvalidArgumentError(
            absl::StrFormat("expected a strictly increasing sequence of "
                            "indices, got [..., %d, %d, ...]",
                            last_index, index)));
        return std::nullopt;
      }
    } else if (index >= size) {
      ctx->set_status(absl::InvalidArgumentError(absl::StrFormat(
          "index is out of range, index=%d >= size=%d", index, size)));
      return std::nullopt;
    }
    last_index = index;
  }
  return IdFilter(size, std::move(raw_indices));
}

}  // namespace arolla
