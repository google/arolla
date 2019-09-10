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

#include <string>
#include <utility>
#include <vector>

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
  for (int i = 0; i < to_types.size(); ++i) {
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
                  const QExprOperatorSignature* operator_signature) const {
    absl::StrAppend(out, operator_signature->name());
  }
};

}  // namespace

absl::StatusOr<const QExprOperatorSignature*> FindMatchingSignature(
    const QExprOperatorSignature* requested_signature,
    absl::Span<const QExprOperatorSignature* const> supported_signatures,
    absl::string_view op_name) {
  std::vector<const QExprOperatorSignature*> matching_qtypes;

  // Save the signature with all types decayed to look for a matching candidate.
  std::vector<QTypePtr> decayed_input_types;
  decayed_input_types.reserve(requested_signature->GetInputTypes().size());
  for (auto input_type : requested_signature->GetInputTypes()) {
    decayed_input_types.push_back(DecayDerivedQType(input_type));
  }

  for (const auto& candidate : supported_signatures) {
    const bool output_types_match =
        DecayDerivedQType(requested_signature->GetOutputType()) ==
        DecayDerivedQType(candidate->GetOutputType());
    if (!CanCastImplicitly(requested_signature->GetInputTypes(),
                           candidate->GetInputTypes()) ||
        !output_types_match) {
      continue;
    }

    // If the candidate fully matches the requested signature with decayed
    // input types, return it.
    if (decayed_input_types == candidate->GetInputTypes()) {
      return candidate;
    }

    std::vector<const QExprOperatorSignature*> new_matching_signatures;
    bool previous_match_is_better = false;
    for (auto previous : matching_qtypes) {
      // If the candidate is castable to a previous one, there is no sense to
      // consider the previous one anymore.
      if (CanCastImplicitly(candidate->GetInputTypes(),
                            previous->GetInputTypes())) {
        continue;
      }

      new_matching_signatures.push_back(previous);
      // If a previous candidate is castable to the current one, there is no
      // sense to consider the current one anymore.
      if (CanCastImplicitly(previous->GetInputTypes(),
                            candidate->GetInputTypes())) {
        previous_match_is_better = true;
      }
    }
    if (!previous_match_is_better) {
      new_matching_signatures.push_back(candidate);
    }
    matching_qtypes = std::move(new_matching_signatures);
  }

  if (matching_qtypes.empty()) {
    return absl::NotFoundError(absl::StrFormat(
        "QExpr operator %s%s not found; %s\n%s", op_name,
        requested_signature->name(), SuggestMissingDependency(),
        SuggestAvailableOverloads(op_name, supported_signatures)));
  }
  if (matching_qtypes.size() > 1) {
    return absl::FailedPreconditionError(absl::StrFormat(
        "ambiguous overloads for the QExpr operator %s%s: provided argument "
        "types can be cast to the following supported signatures: %s ",
        op_name, requested_signature->name(),
        absl::StrJoin(matching_qtypes, ", ", SignatureFormatter())));
  }
  return matching_qtypes.at(0);
}

}  // namespace arolla
