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
#include "arolla/qexpr/casting.h"

#include <cstddef>
#include <string>

#include "absl/container/inlined_vector.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/qexpr/operator_errors.h"
#include "arolla/qexpr/qexpr_operator_signature.h"
#include "arolla/qtype/derived_qtype.h"
#include "arolla/qtype/qtype.h"
#include "arolla/qtype/standard_type_properties/common_qtype.h"

namespace arolla {
namespace {

bool CanCastImplicitly(absl::Span<const QTypePtr> from_types,
                       absl::Span<const QTypePtr> to_types) {
  if (from_types.size() != to_types.size()) {
    return false;
  }
  for (size_t i = 0; i < to_types.size(); ++i) {
    // We enable_broadcasting here, as we expect that the compiler is capable to
    // broadcast the arguments.
    if (!CanCastImplicitly(from_types[i], to_types[i],
                           /*enable_broadcasting=*/true)) {
      return false;
    }
  }
  return true;
}

struct SignatureFormatter {
  void operator()(std::string* out,
                  const QExprOperatorSignature* signature) const {
    absl::StrAppend(out, signature);
  }
};

}  // namespace

absl::StatusOr<const QExprOperatorSignature*> FindMatchingSignature(
    absl::Span<const QTypePtr> input_types, QTypePtr output_type,
    absl::Span<const QExprOperatorSignature* const> supported_signatures,
    absl::string_view op_name) {
  // Save the signature with all types decayed to look for a matching candidate.
  const QTypePtr decayed_output_type = DecayDerivedQType(output_type);
  absl::InlinedVector<QTypePtr, 6> decayed_input_types(input_types.size());
  for (size_t i = 0; i < input_types.size(); ++i) {
    decayed_input_types[i] = DecayDerivedQType(input_types[i]);
  }

  absl::InlinedVector<const QExprOperatorSignature*, 8> frontier;
  for (const auto& candidate : supported_signatures) {
    if (decayed_output_type != DecayDerivedQType(candidate->output_type())) {
      continue;
    }
    if (!CanCastImplicitly(input_types, candidate->input_types())) {
      continue;
    }
    // If the candidate fully matches the requested signature with decayed
    // input types, return it.
    if (decayed_input_types == candidate->input_types()) {
      return candidate;
    }
    bool dominates = false;
    bool dominated = false;
    auto out_it = frontier.begin();
    for (auto* previous : frontier) {
      if (CanCastImplicitly(candidate->input_types(),
                            previous->input_types())) {
        dominates = true;  // Discarding previous candidate.
      } else if (dominates || !CanCastImplicitly(previous->input_types(),
                                                 candidate->input_types())) {
        *out_it++ = previous;  // Keeping previous candidate.
      } else {
        dominated = true;  // Discarding current candidate.
        break;
      }
    }
    if (dominates) {
      frontier.erase(out_it, frontier.end());
    }
    if (!dominated) {
      frontier.push_back(candidate);
    }
  }
  if (frontier.empty()) {
    return absl::NotFoundError(absl::StrFormat(
        "QExpr operator %s%v not found; %s\n%s", op_name,
        QExprOperatorSignature::Get(input_types, output_type),
        SuggestMissingDependency(),
        SuggestAvailableOverloads(op_name, supported_signatures)));
  }
  if (frontier.size() > 1) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "ambiguous overloads for the QExpr operator %s%v: provided argument "
        "types can be cast to the following supported signatures: %s ",
        op_name, QExprOperatorSignature::Get(input_types, output_type),
        absl::StrJoin(frontier, ", ", SignatureFormatter())));
  }
  return frontier[0];
}

}  // namespace arolla
