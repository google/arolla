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
#include "arolla/naming/policy.h"

#include <string>
#include <vector>

#include "absl/base/no_destructor.h"
#include "absl/status/status.h"
#include "absl/status/statusor.h"
#include "absl/strings/ascii.h"
#include "absl/strings/match.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_replace.h"
#include "absl/strings/string_view.h"
#include "absl/strings/strip.h"
#include "arolla/naming/protopath_id.h"
#include "arolla/naming/table.h"

namespace arolla::naming {

// Implementation of a Policy. One subclass per policy name.
class PolicyImpl {
 public:
  explicit PolicyImpl(absl::string_view policy_name)
      : policy_name_(policy_name) {}

  virtual ~PolicyImpl() = default;

  virtual std::string Format(const ColumnPath& path) const = 0;

  virtual std::string Format(const TablePath& path) const = 0;

  const std::string& Name() const { return policy_name_; }

 private:
  std::string policy_name_;
};

const std::string& Policy::Name() const { return policy_impl_->Name(); }

std::string Policy::Format(const ColumnPath& path) const {
  return policy_impl_->Format(path);
}

std::string Policy::Format(const TablePath& path) const {
  return policy_impl_->Format(path);
}

namespace {

class DefaultPolicyImpl : public PolicyImpl {
 public:
  DefaultPolicyImpl() : PolicyImpl(kDefaultPolicyName) {}

  std::string Format(const TablePath& table_path) const override {
    return std::string(table_path.FullName());
  }
  std::string Format(const ColumnPath& column_path) const override {
    return std::string(column_path.FullName());
  }
};

class DoubleUnderscorePolicyImpl : public PolicyImpl {
 public:
  DoubleUnderscorePolicyImpl() : PolicyImpl(kDoubleUnderscorePolicyName) {}

  std::string Format(const TablePath& table_path) const override {
    return Format(table_path.PathSegments());
  }
  std::string Format(const ColumnPath& column_path) const override {
    return Format(column_path.PathSegments());
  }

 private:
  // RLv1 compatible name mangling for extension field name.
  // Example: "Ext::foo_pkg.BarType.baz_field" => "foo_pkg_bartype_baz_field"
  static std::string MangleExtensionFieldName(absl::string_view field_name) {
    if (absl::ConsumePrefix(&field_name, kExtensionFieldPrefix)) {
      return absl::StrReplaceAll(absl::AsciiStrToLower(field_name),
                                 {{".", "_"}});
    } else {
      return std::string(field_name);
    }
  }

  std::string Format(const std::vector<PathSegment>& segments) const {
    return absl::StrJoin(
        segments, "__", [](std::string* ret, const PathSegment& segment) {
          std::string field_name =
              absl::StrReplaceAll(segment.FieldName(), {{"/", "__"}});
          absl::StrAppend(ret, MangleExtensionFieldName(field_name));
        });
  }
};

class SingleUnderscorePolicyImpl : public PolicyImpl {
 public:
  SingleUnderscorePolicyImpl() : PolicyImpl(kSingleUnderscorePolicyName) {}

  std::string Format(const TablePath& table_path) const override {
    return Reformat(table_path.FullName());
  }
  std::string Format(const ColumnPath& column_path) const override {
    return Reformat(column_path.FullName());
  }

 private:
  std::string Reformat(absl::string_view name) const {
    if (name.empty()) return "";
    return absl::StrReplaceAll(name.substr(1), {{"/", "_"}});
  }
};

class LeafOnlyPolicyImpl : public PolicyImpl {
 public:
  LeafOnlyPolicyImpl() : PolicyImpl(kLeafOnlyPolicyName) {}

  std::string Format(const TablePath& table_path) const override {
    return Reformat(table_path.FullName());
  }
  std::string Format(const ColumnPath& column_path) const override {
    return Reformat(column_path.FullName());
  }

 private:
  std::string Reformat(absl::string_view name) const {
    return std::string(absl::EndsWith(name, "@size")
                           ? name
                           : name.substr(name.find_last_of('/') + 1));
  }
};

class ProtopathIdPolicyImpl : public PolicyImpl {
 public:
  ProtopathIdPolicyImpl() : PolicyImpl(kProtopathIdPolicyName) {}

  std::string Format(const TablePath& table_path) const override {
    return TablePathToProtopathId(table_path);
  }
  std::string Format(const ColumnPath& column_path) const override {
    return ColumnPathToProtopathId(column_path);
  }
};

class GoogleSQLPolicyImpl : public PolicyImpl {
 public:
  GoogleSQLPolicyImpl() : PolicyImpl(kGoogleSQLPolicyName) {}

  std::string Format(const TablePath& table_path) const override {
    return Format(table_path.PathSegments());
  }
  std::string Format(const ColumnPath& column_path) const override {
    return Format(column_path.PathSegments());
  }

 private:
  std::string Format(const std::vector<PathSegment>& segments) const {
    return absl::StrJoin(
        segments, ".", [](std::string* ret, const PathSegment& segment) {
          absl::string_view field_name = segment.FieldName();
          if (absl::ConsumePrefix(&field_name, kExtensionFieldPrefix)) {
            absl::StrAppend(ret, "(", field_name, ")");
          } else {
            absl::StrAppend(ret, field_name);
          }
        });
  }
};

}  // namespace

Policy DefaultPolicy() {
  static const absl::NoDestructor<DefaultPolicyImpl> impl;
  return Policy{*impl};
}

Policy DoubleUnderscorePolicy() {
  static const absl::NoDestructor<DoubleUnderscorePolicyImpl> impl;
  return Policy{*impl};
}

Policy SingleUnderscorePolicy() {
  static const absl::NoDestructor<SingleUnderscorePolicyImpl> impl;
  return Policy{*impl};
}

Policy LeafOnlyPolicy() {
  static const absl::NoDestructor<LeafOnlyPolicyImpl> impl;
  return Policy{*impl};
}

Policy ProtopathIdPolicy() {
  static const absl::NoDestructor<ProtopathIdPolicyImpl> impl;
  return Policy{*impl};
}

Policy GoogleSQLPolicy() {
  static const absl::NoDestructor<GoogleSQLPolicyImpl> impl;
  return Policy{*impl};
}

absl::StatusOr<Policy> GetPolicy(absl::string_view policy_name) {
  if (policy_name == kDefaultPolicyName) {
    return DefaultPolicy();
  }
  if (policy_name == kDoubleUnderscorePolicyName) {
    return DoubleUnderscorePolicy();
  }
  if (policy_name == kSingleUnderscorePolicyName) {
    return SingleUnderscorePolicy();
  }
  if (policy_name == kLeafOnlyPolicyName) {
    return LeafOnlyPolicy();
  }
  if (policy_name == kProtopathIdPolicyName) {
    return ProtopathIdPolicy();
  }
  if (policy_name == kGoogleSQLPolicyName) {
    return GoogleSQLPolicy();
  }
  return absl::InvalidArgumentError(
      absl::StrFormat("undefined naming policy: %s", policy_name));
}

}  // namespace arolla::naming
