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
#include "arolla/naming/table.h"

#include <cstddef>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/string_view.h"
#include "absl/types/span.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::naming {

namespace {

// Returns a formatted path string.
// If `show_index_markers` is true, includes the index markers of the segments.
std::string FormatSegments(const std::vector<PathSegment>& segments,
                           bool show_index_markers) {
  std::string ret;
  for (const PathSegment& segment : segments) {
    ret += '/';
    ret += segment.FieldName();
    if (show_index_markers && segment.IsIndex()) {
      ret += std::string(kIndexMarker);
    }
  }
  return ret;
}

}  // namespace

std::string PathSegment::DebugString() const {
  return absl::StrFormat("PathSegment(\"%s\", is_index=%s)", field_name_,
                         is_index_ ? "True" : "False");
}

ColumnPath TablePath::Column(PathSegment segment) const {
  std::vector<PathSegment> segments{path_segments_};
  segments.emplace_back(std::move(segment));
  return ColumnPath(std::move(segments));
}

ColumnPath TablePath::Column(absl::string_view name, bool is_index) const {
  return Column(PathSegment(name, is_index));
}

ColumnPath TablePath::Column(const ColumnPath& column) const {
  std::vector<PathSegment> segments;
  segments.reserve(path_segments_.size() + column.PathSegments().size());
  for (const PathSegment& path_segment : path_segments_) {
    segments.push_back(path_segment);
  }
  for (const PathSegment& path_segment : column.PathSegments()) {
    segments.push_back(path_segment);
  }
  return ColumnPath(std::move(segments));
}

ColumnPath TablePath::Size(absl::string_view name) const {
  return name.empty() ? Column(kSizeColumnName)
                      : Child(name).Column(kSizeColumnName);
}

ColumnPath TablePath::Size(const TablePath& child) const {
  return Child(child).Column(kSizeColumnName);
}

std::string TablePath::FullName(bool show_index_markers) const {
  return FormatSegments(path_segments_, show_index_markers);
}

std::string ColumnPath::FullName(bool show_index_markers) const {
  return FormatSegments(path_segments_, show_index_markers);
}

std::string TablePath::DebugString() const {
  return absl::StrFormat("TablePath(\"%s\")",
                         FormatSegments(path_segments_, true));
}

std::string ColumnPath::DebugString() const {
  return absl::StrFormat("ColumnPath(\"%s\")",
                         FormatSegments(path_segments_, true));
}

std::optional<TablePath> TablePath::ParentIndexPath() const {
  std::vector<PathSegment> segments{path_segments_};
  if (segments.empty()) {
    // Root path does not have a parent.
    return std::nullopt;
  }
  if (segments.back().IsIndex()) {
    // Don't treat the path itself as a parent.
    segments.pop_back();
  }
  while (!segments.empty() && !segments.back().IsIndex()) {
    segments.pop_back();
  }
  return TablePath(std::move(segments));
}

TablePath ColumnPath::ParentIndexPath() const {
  std::vector<PathSegment> segments = {path_segments_};
  while (!segments.empty() && !segments.back().IsIndex()) {
    segments.pop_back();
  }
  return TablePath(std::move(segments));
}

namespace {

absl::StatusOr<std::vector<PathSegment>> RemovePrefixSegments(
    const std::vector<PathSegment>& path_segments, const TablePath& prefix) {
  absl::Span<const PathSegment> segs = absl::MakeSpan(path_segments);
  const size_t prefix_len = prefix.PathSegments().size();
  if (segs.subspan(0, prefix_len) != absl::MakeSpan(prefix.PathSegments())) {
    return absl::InvalidArgumentError(
        absl::StrFormat("%s must be a prefix of %s", prefix.DebugString(),
                        FormatSegments(path_segments, true)));
  }
  absl::Span<const PathSegment> suffix = segs.subspan(prefix_len);
  return std::vector<PathSegment>(suffix.begin(), suffix.end());
}

}  // namespace

absl::StatusOr<TablePath> TablePath::RemovePrefix(
    const TablePath& prefix) const {
  ASSIGN_OR_RETURN(std::vector<PathSegment> suffix,
                   RemovePrefixSegments(PathSegments(), prefix));
  return TablePath(std::move(suffix));
}

absl::StatusOr<ColumnPath> ColumnPath::RemovePrefix(
    const TablePath& prefix) const {
  ASSIGN_OR_RETURN(std::vector<PathSegment> suffix,
                   RemovePrefixSegments(PathSegments(), prefix));
  return ColumnPath(std::move(suffix));
}

}  // namespace arolla::naming
