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
#include "arolla/codegen/operator_package/operator_package.h"

#include <set>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "google/protobuf/io/gzip_stream.h"
#include "google/protobuf/io/zero_copy_stream_impl_lite.h"
#include "arolla/codegen/operator_package/operator_package.pb.h"
#include "arolla/expr/expr_operator.h"
#include "arolla/expr/registered_expr_operator.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_value.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization/encode.h"
#include "arolla/serialization_codecs/generic/operator_codec.pb.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::operator_package {

using ::arolla::expr::ExprOperatorPtr;
using ::arolla::expr::ExprOperatorRegistry;
using ::arolla::expr::LookupOperator;
using ::arolla::serialization::Decode;
using ::arolla::serialization::Encode;
using ::arolla::serialization_codecs::OperatorV1Proto;

absl::Status ParseEmbeddedOperatorPackage(
    absl::string_view embedded_zlib_data,
    OperatorPackageProto* operator_package_proto) {
  ::google::protobuf::io::ArrayInputStream input_stream(embedded_zlib_data.data(),
                                              embedded_zlib_data.size());
  ::google::protobuf::io::GzipInputStream gzip_input_stream(&input_stream);
  if (!operator_package_proto->ParseFromZeroCopyStream(&gzip_input_stream) ||
      gzip_input_stream.ZlibErrorMessage() != nullptr) {
    return absl::InternalError("unable to parse an embedded operator package");
  }
  return absl::OkStatus();
}

absl::Status LoadOperatorPackageProto(
    const OperatorPackageProto& operator_package_proto) {
  if (operator_package_proto.version() != 1) {
    return absl::InvalidArgumentError(
        absl::StrFormat("expected operator_package_proto.version=1, got %d",
                        operator_package_proto.version()));
  }

  // Check dependencies.
  auto* const operator_registry = ExprOperatorRegistry::GetInstance();
  auto check_registered_operator_presence = [&](absl::string_view name) {
    return operator_registry->LookupOperatorOrNull(name) != nullptr;
  };
  std::set<absl::string_view> missing_operators;
  for (absl::string_view operator_name :
       operator_package_proto.required_registered_operators()) {
    if (!check_registered_operator_presence(operator_name)) {
      missing_operators.insert(operator_name);
    }
  }
  if (!missing_operators.empty()) {
    return absl::FailedPreconditionError(
        "missing dependencies: M." + absl::StrJoin(missing_operators, ", M."));
  }

  // Check operators that already registered.
  std::set<absl::string_view> already_registered_operators;
  for (const auto& operator_proto : operator_package_proto.operators()) {
    if (check_registered_operator_presence(
            operator_proto.registration_name())) {
      already_registered_operators.insert(operator_proto.registration_name());
    }
  }

  if (!already_registered_operators.empty()) {
    return absl::FailedPreconditionError(
        "already present in the registry: M." +
        absl::StrJoin(already_registered_operators, ", M."));
  }

  // Load operators.
  for (int i = 0; i < operator_package_proto.operators_size(); ++i) {
    const auto& operator_proto = operator_package_proto.operators(i);
    ASSIGN_OR_RETURN(
        auto decode_result, Decode(operator_proto.implementation()),
        _ << "operators[" << i
          << "].registration_name=" << operator_proto.registration_name());
    if (decode_result.values.size() != 1 || !decode_result.exprs.empty()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected to get a value, got %d values and %d exprs; "
          "operators[%d].registration_name=%s",
          decode_result.values.size(), decode_result.exprs.size(), i,
          operator_proto.registration_name()));
    }
    const auto& qvalue = decode_result.values[0];
    if (qvalue.GetType() != GetQType<ExprOperatorPtr>()) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected to get %s, got %s; operators[%d].registration_name=%s",
          GetQType<ExprOperatorPtr>()->name(), qvalue.GetType()->name(), i,
          operator_proto.registration_name()));
    }
    RETURN_IF_ERROR(operator_registry
                        ->Register(operator_proto.registration_name(),
                                   qvalue.UnsafeAs<ExprOperatorPtr>())
                        .status());
  }
  return absl::OkStatus();
}

absl::StatusOr<OperatorPackageProto> DumpOperatorPackageProto(
    absl::Span<const absl::string_view> operator_names) {
  OperatorPackageProto result;
  result.set_version(1);
  // Store operator implementations.
  std::set<absl::string_view> stored_operators;
  for (const auto& op_name : operator_names) {
    if (!stored_operators.emplace(op_name).second) {
      return absl::InvalidArgumentError(
          absl::StrFormat("operator `%s` is listed multiple times", op_name));
    }
    auto* op_proto = result.add_operators();
    op_proto->set_registration_name(op_name.data(), op_name.size());
    ASSIGN_OR_RETURN(const auto& op, LookupOperator(op_name));
    ASSIGN_OR_RETURN(const auto& op_impl, op->GetImplementation());
    ASSIGN_OR_RETURN(*op_proto->mutable_implementation(),
                     Encode({TypedValue::FromValue(op_impl)}, {}));
  }
  // We introspect the serialized data to identify the required set of
  // registered operators.
  std::set<absl::string_view> required_registered_operators;
  for (const auto& op_proto : result.operators()) {
    if (required_registered_operators.count(op_proto.registration_name())) {
      return absl::InvalidArgumentError(absl::StrFormat(
          "expected the operator names to be given in topological order, but "
          "`%s` is listed after it was already required by other operator",
          op_proto.registration_name()));
    }
    for (const auto& decoding_step :
         op_proto.implementation().decoding_steps()) {
      if (!decoding_step.has_value() ||
          !decoding_step.value().HasExtension(OperatorV1Proto::extension)) {
        continue;
      }
      const auto& op_v1_proto =
          decoding_step.value().GetExtension(OperatorV1Proto::extension);
      if (!op_v1_proto.has_registered_operator_name()) {
        continue;
      }
      required_registered_operators.emplace(
          op_v1_proto.registered_operator_name());
    }
  }
  for (const auto& op_proto : result.operators()) {
    required_registered_operators.erase(op_proto.registration_name());
  }
  for (const auto& op_name : required_registered_operators) {
    result.add_required_registered_operators(op_name.data(), op_name.size());
  }
  return result;
}

}  // namespace arolla::operator_package
