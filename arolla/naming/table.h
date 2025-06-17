// Copyright 2025 Google LLC
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
#ifndef AROLLA_NAMING_TABLE_H_
#define AROLLA_NAMING_TABLE_H_

#include <cstddef>
#include <optional>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "absl/hash/hash.h"
#include "absl/status/statusor.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "arolla/util/types.h"

namespace arolla::naming {

// Name of size column.
inline constexpr absl::string_view kSizeColumnName = "@size";

inline constexpr absl::string_view kExtensionFieldPrefix = "Ext::";

// The segment suffix to indicate that a segment corresponds to an index type.
// Not printed in the default naming policy.
inline constexpr absl::string_view kIndexMarker = "[:]";

// Returns string used for naming for the access to the specified field.
inline std::string FieldAccess(absl::string_view field_name) {
  return std::string(field_name);
}

// Returns string used for naming for the access to the map.
inline std::string MapAccess(absl::string_view field_name,
                             absl::string_view key) {
  return absl::StrCat(field_name, "[\"", key, "\"]");
}

// Returns string used for naming for the access to the array.
inline std::string ArrayAccess(absl::string_view field_name, size_t idx) {
  return absl::StrCat(field_name, "[", idx, "]");
}

// Returns string used for naming for the access to proto extension
// by fully qualified extension field name.
inline std::string ProtoExtensionAccess(absl::string_view ext_name) {
  return absl::StrCat(kExtensionFieldPrefix, ext_name);
}

// A piece between two slashes in a path.
class PathSegment {
 public:
  // Constructs a path segment from a field name and whether the segment
  // corresponds to an index type.
  PathSegment(absl::string_view field_name, bool is_index)
      : field_name_(field_name), is_index_(is_index) {}

  // Returns the field name part, which should not include meta characters
  // such as slash or the index marker.
  const std::string& FieldName() const { return field_name_; }

  bool IsExtension() const {
    return absl::StartsWith(field_name_, kExtensionFieldPrefix);
  }

  // Returns whether the segment corresponds to an index type, which also
  // means a repeated field in proto or a feature dimension.
  bool IsIndex() const { return is_index_; }

  template <typename H>
  friend H AbslHashValue(H h, const PathSegment& s) {
    return H::combine(std::move(h), s.field_name_, s.is_index_);
  }

  signed_size_t PythonHash() const { return absl::Hash<PathSegment>()(*this); }

  std::string DebugString() const;

 private:
  std::string field_name_;
  bool is_index_;
};

inline bool operator==(const PathSegment& a, const PathSegment& b) {
  return std::pair(a.FieldName(), a.IsIndex()) ==
         std::pair(b.FieldName(), b.IsIndex());
}

inline bool operator!=(const PathSegment& a, const PathSegment& b) {
  return std::pair(a.FieldName(), a.IsIndex()) !=
         std::pair(b.FieldName(), b.IsIndex());
}

inline bool operator<(const PathSegment& a, const PathSegment& b) {
  return std::pair(a.FieldName(), a.IsIndex()) <
         std::pair(b.FieldName(), b.IsIndex());
}

class ColumnPath;

// Class encapsulating naming used for the Table.
class TablePath {
 public:
  // Constructs TablePath with empty name (root path).
  TablePath() = default;

  // Constructs TablePath with a sequence of path segments.
  explicit TablePath(std::vector<PathSegment> path_segments)
      : path_segments_(std::move(path_segments)) {}

  // Constructs TablePath that consists of a single segment.
  explicit TablePath(absl::string_view name, bool is_index = false)
      : TablePath({PathSegment(name, is_index)}) {}

  // Returns a TablePath with a child segment appended to this TablePath.
  TablePath Child(PathSegment path_segment) const& {
    std::vector<PathSegment> segments{path_segments_};
    segments.push_back(std::move(path_segment));
    return TablePath{std::move(segments)};
  }
  TablePath Child(PathSegment path_segment) && {
    path_segments_.push_back(std::move(path_segment));
    return TablePath{std::move(path_segments_)};
  }
  TablePath Child(absl::string_view name, bool is_index = false) const& {
    return this->Child(PathSegment(name, is_index));
  }
  TablePath Child(absl::string_view name, bool is_index = false) && {
    return std::move(*this).Child(PathSegment(name, is_index));
  }

  // Concatenates another TablePath to this TablePath.
  TablePath Child(const TablePath& suffix) const {
    TablePath ret = *this;
    for (const PathSegment& path_segment : suffix.path_segments_) {
      ret = std::move(ret).Child(path_segment);
    }
    return ret;
  }

  // Returns a ColumnPath with a last segment appended to this TablePath.
  ColumnPath Column(PathSegment path_segment) const;
  ColumnPath Column(absl::string_view name, bool is_index = false) const;
  ColumnPath Column(const ColumnPath& column) const;

  // Returns column accessing to the sizes of child items of each item.
  ColumnPath Size(absl::string_view name) const;
  ColumnPath Size(const TablePath& child) const;

  // Returns column accessing to the keys of the table map.
  TablePath MapKeys() const { return Child("@key", false); }

  // Returns column accessing to the values of the table map.
  TablePath MapValues() const { return Child("@value", false); }

  // Returns the sequence of segments that represents this path.
  const std::vector<PathSegment>& PathSegments() const {
    return path_segments_;
  }

  // Returns full name of the TablePath.
  std::string FullName() const;

  // Returns a TablePath corresponding to the closest ancestor index type.
  // The root path does not have a parent.
  std::optional<TablePath> ParentIndexPath() const;

  // Removes a prefix from the path and returns the suffix. If the path does
  // not starts with the prefix, error is returned.
  absl::StatusOr<TablePath> RemovePrefix(const TablePath& prefix) const;

  template <typename H>
  friend H AbslHashValue(H h, const TablePath& t) {
    return H::combine(std::move(h), t.path_segments_);
  }

  signed_size_t PythonHash() const { return absl::Hash<TablePath>()(*this); }

  std::string DebugString() const;

 private:
  std::vector<PathSegment> path_segments_;
};

// Class encapsulating naming used for the Column of a Table.
class ColumnPath {
 public:
  // Constructs an empty (invalid) ColumnPath. Needed by CLIF.
  ColumnPath() = default;

  // Constructs ColumnPath with a sequence of path segments.
  explicit ColumnPath(std::vector<PathSegment> path_segments)
      : path_segments_(std::move(path_segments)) {}

  // Constructs ColumnPath that consists of a single segment.
  explicit ColumnPath(absl::string_view name, bool is_index = false)
      : ColumnPath({PathSegment(name, is_index)}) {}

  // Returns the sequence of segments that represents this path.
  const std::vector<PathSegment>& PathSegments() const {
    return path_segments_;
  }

  // Returns full name of the ColumnPath.
  std::string FullName() const;

  // Returns a TablePath corresponding to the index type to which the feature
  // belongs.
  TablePath ParentIndexPath() const;

  // Removes a prefix from the path and returns the suffix. If the path does
  // not starts with the prefix, error is returned.
  absl::StatusOr<ColumnPath> RemovePrefix(const TablePath& prefix) const;

  std::string DebugString() const;

  template <typename H>
  friend H AbslHashValue(H h, const ColumnPath& c) {
    return H::combine(std::move(h), c.FullName());
  }

  signed_size_t PythonHash() const { return absl::Hash<ColumnPath>()(*this); }

  friend std::ostream& operator<<(std::ostream& stream,
                                  const ColumnPath& path) {
    stream << path.DebugString();
    return stream;
  }

 private:
  std::vector<PathSegment> path_segments_;
};

inline bool operator==(const TablePath& a, const TablePath& b) {
  return a.PathSegments() == b.PathSegments();
}
inline bool operator!=(const TablePath& a, const TablePath& b) {
  return a.PathSegments() != b.PathSegments();
}
inline bool operator<(const TablePath& a, const TablePath& b) {
  return a.PathSegments() < b.PathSegments();
}
inline bool operator==(const ColumnPath& a, const ColumnPath& b) {
  return a.PathSegments() == b.PathSegments();
}
inline bool operator!=(const ColumnPath& a, const ColumnPath& b) {
  return a.PathSegments() != b.PathSegments();
}
inline bool operator<(const ColumnPath& a, const ColumnPath& b) {
  return a.PathSegments() < b.PathSegments();
}

}  // namespace arolla::naming

#endif  // AROLLA_NAMING_TABLE_H_
