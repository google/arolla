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
#include "arolla/qexpr/operator_metadata.h"

#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/container/flat_hash_map.h"
#include "absl/log/log.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

absl::Status QExprOperatorMetadataRegistry::AddOperatorFamilyMetadata(
    QExprOperatorFamilyMetadata metadata) {
  absl::WriterMutexLock lock(&mutex_);
  if (family_metadatas_.contains(metadata.name) ||
      operator_metadatas_.contains(metadata.name)) {
    return absl::Status(
        absl::StatusCode::kAlreadyExists,
        absl::StrFormat("trying to register individual operator or operator "
                        "family metadata twice under the same name %s",
                        metadata.name));
  }
  family_metadatas_.emplace(metadata.name, std::move(metadata));
  return absl::OkStatus();
}

absl::Status QExprOperatorMetadataRegistry::AddOperatorMetadata(
    QExprOperatorMetadata metadata) {
  absl::WriterMutexLock lock(&mutex_);
  if (family_metadatas_.contains(metadata.name)) {
    return absl::Status(
        absl::StatusCode::kAlreadyExists,
        absl::StrFormat("trying to register individual operator or operator "
                        "family metadata twice under the same name %s",
                        metadata.name));
  }
  auto [iter, inserted] =
      operator_metadatas_.emplace(metadata.name, TypeToMetadata{});
  if (iter->second.contains(metadata.input_qtypes)) {
    return absl::Status(
        absl::StatusCode::kAlreadyExists,
        absl::StrFormat("trying to register operator metadata twice "
                        "for operator %s with input types %s",
                        metadata.name,
                        FormatTypeVector(metadata.input_qtypes)));
  }
  iter->second.emplace(metadata.input_qtypes, std::move(metadata));
  return absl::OkStatus();
}

absl::StatusOr<QExprOperatorMetadata>
QExprOperatorMetadataRegistry::LookupOperatorMetadata(
    absl::string_view op_name, absl::Span<const QTypePtr> input_qtypes) const {
  absl::ReaderMutexLock lock(&mutex_);

  std::vector<QTypePtr> input_qtypes_vector(input_qtypes.begin(),
                                            input_qtypes.end());
  if (auto m = family_metadatas_.find(op_name); m != family_metadatas_.end()) {
    return QExprOperatorMetadata{
        .name = std::string(m->second.name),
        .input_qtypes = std::move(input_qtypes_vector),
        .build_details = m->second.family_build_details};
  }
  if (auto oms = operator_metadatas_.find(op_name);
      oms != operator_metadatas_.end()) {
    if (auto m = oms->second.find(input_qtypes_vector);
        m != oms->second.end()) {
      return m->second;
    }
  }
  return absl::Status(
      absl::StatusCode::kNotFound,
      absl::StrFormat(
          "no metadata is available for operator %s with input types %s",
          op_name, FormatTypeVector(input_qtypes)));
}

QExprOperatorMetadataRegistry& QExprOperatorMetadataRegistry::GetInstance() {
  static absl::NoDestructor<QExprOperatorMetadataRegistry> instance;
  return *instance;
}

absl::flat_hash_map<std::string, std::set<std::string>>
QExprOperatorMetadataRegistry::OperatorBuildDependencies() const {
  absl::flat_hash_map<std::string, std::set<std::string>> result;

  absl::ReaderMutexLock lock(&mutex_);
  for (const auto& [_, metadata] : family_metadatas_) {
    result[absl::StrCat(metadata.name, "(...)")].insert(
        metadata.family_build_details.build_target);
  }
  for (const auto& [name, type_to_meta] : operator_metadatas_) {
    for (const auto& [types, metadata] : type_to_meta) {
      std::string name_with_types =
          absl::StrCat(name, ::arolla::FormatTypeVector(types));
      result[name_with_types].insert(metadata.build_details.build_target);
    }
  }
  return result;
}

int RegisterOperatorFamilyMetadataOrDie(QExprOperatorFamilyMetadata metadata) {
  auto status =
      QExprOperatorMetadataRegistry::GetInstance().AddOperatorFamilyMetadata(
          std::move(metadata));
  if (!status.ok()) {
    // It happens only if the set of operators is configured improperly, so
    // likely will be caught in tests.
    LOG(FATAL) << status;
  }
  return 57;
}

int RegisterOperatorMetadataOrDie(QExprOperatorMetadata metadata) {
  auto status =
      QExprOperatorMetadataRegistry::GetInstance().AddOperatorMetadata(
          std::move(metadata));
  if (!status.ok()) {
    // It happens only if the set of operators is configured improperly, so
    // likely will be caught in tests.
    LOG(FATAL) << status;
  }
  return 57;
}

}  // namespace arolla
