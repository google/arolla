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
#ifndef AROLLA_CODEGEN_OPERATOR_PACKAGE_LOAD_OPERATOR_PACKAGE_H_
#define AROLLA_CODEGEN_OPERATOR_PACKAGE_LOAD_OPERATOR_PACKAGE_H_

#include "absl/status/status.h"
#include "absl/strings/string_view.h"
#include "arolla/codegen/operator_package/operator_package.pb.h"

namespace arolla::operator_package {

// Parse an embedded operator package data compressed using ZLib (or GZip).
absl::Status ParseEmbeddedOperatorPackage(
    absl::string_view embedded_zlib_data,
    OperatorPackageProto* operator_package_proto);

// Loads expr operators from the operator package to the registry.
absl::Status LoadOperatorPackage(
    const OperatorPackageProto& operator_package_proto);

}  // namespace arolla::operator_package

#endif  // AROLLA_CODEGEN_OPERATOR_PACKAGE_LOAD_OPERATOR_PACKAGE_H_
