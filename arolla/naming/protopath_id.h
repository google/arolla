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
#ifndef AROLLA_NAMING_PROTOPATH_ID_H_
#define AROLLA_NAMING_PROTOPATH_ID_H_

#include <string>

#include "absl//status/statusor.h"
#include "absl//strings/string_view.h"
#include "arolla/naming/table.h"

namespace arolla::naming {

// Formats a TablePath as a ProtopathId string.
std::string TablePathToProtopathId(const TablePath& table_path);

// Formats a ColumnPath as a ProtopathId string.
std::string ColumnPathToProtopathId(const ColumnPath& column_path);

// Parses a ProtopathId string into a TablePath.
absl::StatusOr<TablePath> TablePathFromProtopathId(
    absl::string_view protopath_id);

// Parses a ProtopathId string into a ColumnPath.
absl::StatusOr<ColumnPath> ColumnPathFromProtopathId(
    absl::string_view protopath_id);

}  // namespace arolla::naming

#endif  // AROLLA_NAMING_PROTOPATH_ID_H_
