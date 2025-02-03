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
#ifndef AROLLA_QEXPR_OPERATOR_METADATA_H_
#define AROLLA_QEXPR_OPERATOR_METADATA_H_

#include <optional>
#include <set>
#include <string>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_map.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/span.h"
#include "arolla/qtype/qtype.h"

namespace arolla {

// Details about the operator functor.
struct OpClassDetails {
  // whenever functor returns absl::StatusOr<T>
  bool returns_status_or;
  // whenever functor accepts EvaluationContext* as the first argument
  bool accepts_context;
  // List of argument ids that can be passed as function returning the value.
  std::vector<int> arg_as_function_ids;
};

// Build system and code generation details about the operator.
struct BuildDetails {
  // The smallest build target that registers the operator in OperatorRegistry.
  std::string build_target;

  // Fully-qualified name of operator class. (Only for operators defined via
  // simple_operator build rule, otherwise empty).
  std::string op_class;

  // Extra information about operator functor. (Only for operators defined via
  // simple_operator build rule, otherwise nullopt).
  std::optional<OpClassDetails> op_class_details;

  // Fully-qualified name of operator family class. (Only for operator families
  // registered via operator_family build rule, otherwise empty).
  std::string op_family_class;

  // Header files needed to instantiate op_class / op_family_class (the one
  // populated).
  std::vector<std::string> hdrs;

  // Build dependencies that contain hdrs.
  std::vector<std::string> deps;
};

// Metadata for QExpr-level operators.
struct QExprOperatorMetadata {
  // Operator name. Required. All registered operators should have distinct
  // name(input_qtypes) signatures.
  std::string name;

  // QTypes of the operator inputs.
  std::vector<QTypePtr> input_qtypes;

  // Build system and code generation details for the operator.
  BuildDetails build_details;
};

// Metadata for QExpr-level operator families.
struct QExprOperatorFamilyMetadata {
  // Operator family name. Required.
  std::string name;

  // Build system and code generation details for the operator family. Is
  // empty if the family is combined from the individual operator instances
  // (see operator_instance_metadatas instead).
  BuildDetails family_build_details = {};
};

// Registry of QExprOperatorMetadata for individual operators and operator
// families.
class QExprOperatorMetadataRegistry final {
 public:
  // Constructs empty QExprOperatorMetadataRegistry. Use GetInstance() instead
  // to get the registry singleton.
  QExprOperatorMetadataRegistry() = default;

  // Add metadata about the whole operator family. Only one such call per
  // operator family is allowed.
  absl::Status AddOperatorFamilyMetadata(QExprOperatorFamilyMetadata metadata);

  // Add metadata about a particular operator instance. Only one such call per
  // operator is allowed.
  absl::Status AddOperatorMetadata(QExprOperatorMetadata metadata);

  // Searches for metadata for the operator with the given name and type.
  absl::StatusOr<QExprOperatorMetadata> LookupOperatorMetadata(
      absl::string_view op_name, absl::Span<const QTypePtr> input_qtypes) const;

  // Returns build dependencies for all registered operators or operator
  // families.
  // Key is *human readable* operator name in the form "OP_NAME(arg_types)".
  // For families (no finite list of types)  "OP_NAME(...)".
  // Examples: math.add(FLOAT32,FLOAT32), core.make_tuple(...)
  //
  // NOTE:
  // This operation is slow and should be used only to produce output for human.
  absl::flat_hash_map<std::string, std::set<std::string>>
  OperatorBuildDependencies() const;

  // Get the QExprOperatorMetadataRegistry instance.
  static QExprOperatorMetadataRegistry& GetInstance();

 private:
  using TypeToMetadata =
      absl::flat_hash_map<std::vector<QTypePtr>, QExprOperatorMetadata>;

  absl::flat_hash_map<std::string, QExprOperatorFamilyMetadata>
      family_metadatas_ ABSL_GUARDED_BY(mutex_);
  absl::flat_hash_map<std::string, TypeToMetadata> operator_metadatas_
      ABSL_GUARDED_BY(mutex_);
  mutable absl::Mutex mutex_;
};

// Registers operator family metadata in the global
// QExprOperatorMetadataRegistry. Fails if a duplicate name is registered. To be
// used for registration in a global variable initializer.
int RegisterOperatorFamilyMetadataOrDie(QExprOperatorFamilyMetadata metadata);

// Registers operator metadata in the global QExprOperatorMetadataRegistry.
// Fails if a duplicate name and type combination is registered. To be used for
// registration in a global variable initializer.
int RegisterOperatorMetadataOrDie(QExprOperatorMetadata metadata);

}  // namespace arolla

#endif  // AROLLA_QEXPR_OPERATOR_METADATA_H_
