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
#include "arolla/proto/reflection/reader.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "absl/log/check.h"
#include "absl/status/status.h"
#include "absl/status/status_matchers.h"
#include "absl/status/statusor.h"
#include "absl/strings/str_format.h"
#include "absl/strings/str_join.h"
#include "google/protobuf/descriptor.h"
#include "arolla/dense_array/dense_array.h"
#include "arolla/dense_array/qtype/types.h"
#include "arolla/memory/frame.h"
#include "arolla/memory/memory_allocation.h"
#include "arolla/memory/optional_value.h"
#include "arolla/proto/test.pb.h"
#include "arolla/proto/types.h"
#include "arolla/qtype/qtype_traits.h"
#include "arolla/qtype/typed_slot.h"
#include "arolla/util/bytes.h"
#include "arolla/util/text.h"
#include "arolla/util/status_macros_backport.h"

namespace arolla::proto {
namespace {

using ::absl_testing::IsOkAndHolds;
using ::testing::ElementsAre;
using ::testing::IsEmpty;

using ProtoRoot = ::testing_namespace::Root;
const auto* kRootDescr = ProtoRoot::descriptor();

auto BuildDescriptorSequence(const std::vector<std::string>& field_names) {
  std::vector<const google::protobuf::FieldDescriptor*> fields;
  const google::protobuf::Descriptor* root_descriptor = kRootDescr;
  for (const auto& name : field_names) {
    CHECK(root_descriptor != nullptr)
        << "incorrect test fields: " << absl::StrJoin(field_names, ",");
    const google::protobuf::FieldDescriptor* field_descriptor =
        root_descriptor->FindFieldByName(name);
    fields.push_back(field_descriptor);
    root_descriptor = field_descriptor->message_type();
  }
  return fields;
}

template <class T>
absl::StatusOr<T> ReadValue(const ProtoTypeReader& reader,
                            const google::protobuf::Message& m, T garbage = T{}) {
  if (reader.qtype() != GetQType<T>()) {
    return absl::FailedPreconditionError(
        absl::StrFormat("QType mismatch: expected %s, found %s",
                        GetQType<T>()->name(), reader.qtype()->name()));
  }
  FrameLayout::Builder layout_builder;
  auto slot = layout_builder.AddSlot<T>();
  ASSIGN_OR_RETURN(auto read_fn, reader.BindReadFn(TypedSlot::FromSlot(slot)));

  FrameLayout memory_layout = std::move(layout_builder).Build();
  MemoryAllocation alloc(&memory_layout);
  FramePtr frame = alloc.frame();
  frame.Set(slot, garbage);

  read_fn(m, frame);
  return frame.Get(slot);
}

template <class T>
absl::StatusOr<OptionalValue<T>> ReadOptionalValue(
    const ProtoTypeReader& reader, const google::protobuf::Message& m) {
  return ReadValue<OptionalValue<T>>(reader, m,
                                     // set present value
                                     T{});
}

template <class T>
absl::StatusOr<OptionalValue<T>> ReadOptionalTopLevelValue(
    const std::string& field_name, const google::protobuf::Message& m) {
  ASSIGN_OR_RETURN(auto reader,
                   ProtoTypeReader::CreateOptionalReader(
                       {kRootDescr->FindFieldByName(field_name)}, {{}}));
  return ReadValue<OptionalValue<T>>(*reader, m);
}

TEST(TopLevelOptionalReaderTest, All) {
  ::testing_namespace::Root m;
  EXPECT_THAT(ReadOptionalTopLevelValue<int32_t>("x", m),
              IsOkAndHolds(std::nullopt));
  m.set_x(19);
  EXPECT_THAT(ReadOptionalTopLevelValue<int32_t>("x", m), IsOkAndHolds(19));
  m.set_x_enum(ProtoRoot::SECOND_VALUE);
  EXPECT_THAT(ReadOptionalTopLevelValue<int32_t>("x_enum", m),
              IsOkAndHolds(ProtoRoot::SECOND_VALUE));
  m.set_str("19");
  EXPECT_THAT(ReadOptionalTopLevelValue<Text>("str", m),
              IsOkAndHolds(Text{"19"}));
  m.set_raw_bytes("19");
  EXPECT_THAT(ReadOptionalTopLevelValue<Bytes>("raw_bytes", m),
              IsOkAndHolds(Bytes{"19"}));
  m.set_x_int64(19);
  EXPECT_THAT(ReadOptionalTopLevelValue<int64_t>("x_int64", m),
              IsOkAndHolds(19));
  m.set_x_uint32(19);
  EXPECT_THAT(ReadOptionalTopLevelValue<int64_t>("x_uint32", m),
              IsOkAndHolds(19));
  m.set_x_uint64(19);
  EXPECT_THAT(ReadOptionalTopLevelValue<uint64_t>("x_uint64", m),
              IsOkAndHolds(19));
  m.set_x_float(19.0f);
  EXPECT_THAT(ReadOptionalTopLevelValue<float>("x_float", m),
              IsOkAndHolds(19.0f));
  m.set_x_double(19.0);
  EXPECT_THAT(ReadOptionalTopLevelValue<double>("x_double", m),
              IsOkAndHolds(19.0));
  m.set_x_fixed64(19);
  EXPECT_THAT(ReadOptionalTopLevelValue<uint64_t>("x_fixed64", m),
              IsOkAndHolds(19));

  // Bytes
  {
    ASSERT_OK_AND_ASSIGN(auto reader,
                         ProtoTypeReader::CreateOptionalReader(
                             {kRootDescr->FindFieldByName("raw_bytes")}, {{}},
                             StringFieldType::kBytes));
    m.set_raw_bytes("19");
    EXPECT_THAT(ReadOptionalValue<Bytes>(*reader, m),
                IsOkAndHolds(Bytes("19")));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto reader, ProtoTypeReader::CreateOptionalReader(
                                          {kRootDescr->FindFieldByName("str")},
                                          {{}}, StringFieldType::kBytes));
    m.set_str("19");
    EXPECT_THAT(ReadOptionalValue<Bytes>(*reader, m),
                IsOkAndHolds(Bytes("19")));
  }
}

template <class T>
absl::StatusOr<::arolla::DenseArray<T>> ReadDenseArrayValue(
    const ProtoTypeReader& reader, const google::protobuf::Message& m) {
  return ReadValue<::arolla::DenseArray<T>>(
      reader, m,
      // garbage
      ::arolla::CreateDenseArray<T>({T{}, T{}}));
}

template <class T>
absl::StatusOr<::arolla::DenseArray<T>> ReadDenseArrayValue(
    const std::vector<std::string>& field_names,
    std::vector<ProtoFieldAccessInfo> access_infos, const google::protobuf::Message& m) {
  ASSIGN_OR_RETURN(auto reader,
                   ProtoTypeReader::CreateDenseArrayReader(
                       BuildDescriptorSequence(field_names), access_infos));
  return ReadDenseArrayValue<T>(*reader, m);
}

TEST(ProtoTypeReader, CreateTopLevelDenseArrayReaderNonRepeatedField) {
  ::testing_namespace::Root m;
  EXPECT_THAT(ReadDenseArrayValue<int32_t>({"x"}, {{}}, m),
              IsOkAndHolds(ElementsAre(std::nullopt)));
  m.set_x(19);
  EXPECT_THAT(ReadDenseArrayValue<int32_t>({"x"}, {{}}, m),
              IsOkAndHolds(ElementsAre(19)));
  m.set_str("19");
  EXPECT_THAT(ReadDenseArrayValue<Text>({"str"}, {{}}, m),
              IsOkAndHolds(ElementsAre("19")));
  m.set_raw_bytes("19");
  EXPECT_THAT(ReadDenseArrayValue<Bytes>({"raw_bytes"}, {{}}, m),
              IsOkAndHolds(ElementsAre("19")));
  m.set_x_int64(19);
  EXPECT_THAT(ReadDenseArrayValue<int64_t>({"x_int64"}, {{}}, m),
              IsOkAndHolds(ElementsAre(19)));
  m.set_x_uint32(19);
  EXPECT_THAT(ReadDenseArrayValue<int64_t>({"x_uint32"}, {{}}, m),
              IsOkAndHolds(ElementsAre(19)));
  m.set_x_uint64(19);
  EXPECT_THAT(ReadDenseArrayValue<uint64_t>({"x_uint64"}, {{}}, m),
              IsOkAndHolds(ElementsAre(19)));
  m.set_x_float(19.0f);
  EXPECT_THAT(ReadDenseArrayValue<float>({"x_float"}, {{}}, m),
              IsOkAndHolds(ElementsAre(19.0f)));
  m.set_x_double(19.0);
  EXPECT_THAT(ReadDenseArrayValue<double>({"x_double"}, {{}}, m),
              IsOkAndHolds(ElementsAre(19.0)));
  // Bytes
  {
    ASSERT_OK_AND_ASSIGN(auto reader,
                         ProtoTypeReader::CreateDenseArrayReader(
                             {kRootDescr->FindFieldByName("raw_bytes")}, {{}},
                             StringFieldType::kBytes));
    m.set_raw_bytes("19");
    EXPECT_THAT(ReadDenseArrayValue<Bytes>(*reader, m),
                IsOkAndHolds(ElementsAre("19")));
  }
  {
    ASSERT_OK_AND_ASSIGN(auto reader, ProtoTypeReader::CreateDenseArrayReader(
                                          {kRootDescr->FindFieldByName("str")},
                                          {{}}, StringFieldType::kBytes));
    m.set_str("19");
    EXPECT_THAT(ReadDenseArrayValue<Bytes>(*reader, m),
                IsOkAndHolds(ElementsAre("19")));
  }
}

template <class T>
absl::StatusOr<OptionalValue<T>> ReadOptionalValue(
    const std::vector<std::string>& field_names,
    std::vector<ProtoFieldAccessInfo> access_infos, const google::protobuf::Message& m) {
  std::vector<const google::protobuf::FieldDescriptor*> fields =
      BuildDescriptorSequence(field_names);
  ASSIGN_OR_RETURN(auto reader,
                   ProtoTypeReader::CreateOptionalReader(fields, access_infos));
  return ReadValue<OptionalValue<T>>(*reader, m);
}

template <class T>
absl::StatusOr<OptionalValue<T>> ReadOptionalValue(
    const std::vector<std::string>& field_names, const google::protobuf::Message& m) {
  return ReadOptionalValue<T>(
      field_names, std::vector<ProtoFieldAccessInfo>(field_names.size()), m);
}

TEST(ProtoTypeReader, CreateInnerOptionalReader) {
  ::testing_namespace::Root m;
  EXPECT_THAT(ReadOptionalValue<int32_t>({"inner", "a"}, m),
              IsOkAndHolds(std::nullopt));
  m.mutable_inner()->set_a(19);
  EXPECT_THAT(ReadOptionalValue<int32_t>({"inner", "a"}, m), IsOkAndHolds(19));

  EXPECT_THAT(ReadOptionalValue<int32_t>({"inner", "inner2", "z"}, m),
              IsOkAndHolds(std::nullopt));
  m.mutable_inner()->mutable_inner2();
  EXPECT_THAT(ReadOptionalValue<int32_t>({"inner", "inner2", "z"}, m),
              IsOkAndHolds(std::nullopt));
  m.mutable_inner()->mutable_inner2()->set_z(19);
  EXPECT_THAT(ReadOptionalValue<int32_t>({"inner", "inner2", "z"}, m),
              IsOkAndHolds(19));
}

template <class T>
absl::StatusOr<OptionalValue<T>> ReadOptionalTopLevelFromRepeatedValue(
    const std::string& field_name, const google::protobuf::Message& m, size_t index = 0) {
  return ReadOptionalValue<T>({field_name}, {RepeatedFieldIndexAccess{index}},
                              m);
}

TEST(ProtoTypeReader, CreateRepeatedIndexAccessOptionalReader) {
  ::testing_namespace::Root m;
  auto read_ys = [](const auto& m) {
    return ReadOptionalValue<int32_t>({"ys"}, {RepeatedFieldIndexAccess{1}}, m);
  };
  EXPECT_THAT(read_ys(m), IsOkAndHolds(std::nullopt));
  m.add_ys(89);
  EXPECT_THAT(read_ys(m), IsOkAndHolds(std::nullopt));
  m.add_ys(77);
  EXPECT_THAT(read_ys(m), IsOkAndHolds(77));

  // inners[:]/a
  auto read_inners_a = [](const auto& m) {
    return ReadOptionalValue<int32_t>({"inners", "a"},
                                      {RepeatedFieldIndexAccess{1}, {}}, m);
  };
  EXPECT_THAT(read_inners_a(m), IsOkAndHolds(std::nullopt));
  m.add_inners();
  EXPECT_THAT(read_inners_a(m), IsOkAndHolds(std::nullopt));
  m.add_inners()->set_a(7);
  EXPECT_THAT(read_inners_a(m), IsOkAndHolds(7));

  // inners[:]/as
  auto read_inners_as = [](const auto& m) {
    return ReadOptionalValue<int32_t>(
        {"inners", "as"},
        {RepeatedFieldIndexAccess{1}, RepeatedFieldIndexAccess{1}}, m);
  };
  m.mutable_inners(1)->add_as(0);
  EXPECT_THAT(read_inners_as(m), IsOkAndHolds(std::nullopt));
  m.mutable_inners(1)->add_as(57);
  EXPECT_THAT(read_inners_as(m), IsOkAndHolds(57));

  // different types
  *m.add_repeated_str() = "19";
  EXPECT_THAT(ReadOptionalTopLevelFromRepeatedValue<Text>("repeated_str", m),
              IsOkAndHolds(Text("19")));
  *m.add_repeated_raw_bytes() = "19";
  EXPECT_THAT(
      ReadOptionalTopLevelFromRepeatedValue<Bytes>("repeated_raw_bytes", m),
      IsOkAndHolds(Bytes("19")));
  m.add_repeated_floats(19.0f);
  EXPECT_THAT(
      ReadOptionalTopLevelFromRepeatedValue<float>("repeated_floats", m),
      IsOkAndHolds(19.0f));
  m.add_repeated_doubles(19.0);
  EXPECT_THAT(
      ReadOptionalTopLevelFromRepeatedValue<double>("repeated_doubles", m),
      IsOkAndHolds(19.0));
  m.add_repeated_int32s(19);
  EXPECT_THAT(ReadOptionalTopLevelFromRepeatedValue<int>("repeated_int32s", m),
              IsOkAndHolds(19));
  m.add_repeated_int64s(19);
  EXPECT_THAT(
      ReadOptionalTopLevelFromRepeatedValue<int64_t>("repeated_int64s", m),
      IsOkAndHolds(19));
  m.add_repeated_uint32s(19);
  EXPECT_THAT(
      ReadOptionalTopLevelFromRepeatedValue<int64_t>("repeated_uint32s", m),
      IsOkAndHolds(19));
  m.add_repeated_uint64s(19);
  EXPECT_THAT(
      ReadOptionalTopLevelFromRepeatedValue<uint64_t>("repeated_uint64s", m),
      IsOkAndHolds(19));
  m.add_repeated_bools(true);
  EXPECT_THAT(ReadOptionalTopLevelFromRepeatedValue<bool>("repeated_bools", m),
              IsOkAndHolds(true));
  m.add_repeated_enums(ProtoRoot::SECOND_VALUE);
  EXPECT_THAT(ReadOptionalTopLevelFromRepeatedValue<int>("repeated_enums", m),
              IsOkAndHolds(ProtoRoot::SECOND_VALUE));
}

template <class T>
absl::StatusOr<::arolla::DenseArray<T>> ReadDenseArrayTopLevelValue(
    const std::string& field_name, const google::protobuf::Message& m) {
  return ReadDenseArrayValue<T>({field_name}, {RepeatedFieldAccess{}}, m);
}

TEST(ProtoTypeReader, CreateRepeatedAccessOptionalReader) {
  ::testing_namespace::Root m;
  EXPECT_THAT(ReadDenseArrayTopLevelValue<int>("ys", m),
              IsOkAndHolds(IsEmpty()));
  m.add_ys(89);
  m.add_ys(57);
  EXPECT_THAT(ReadDenseArrayTopLevelValue<int>("ys", m),
              IsOkAndHolds(ElementsAre(89, 57)));

  // inners[:]/a
  auto read_inners_a = [](const auto& m) {
    return ReadDenseArrayValue<int32_t>({"inners", "a"},
                                        {RepeatedFieldAccess{}, {}}, m);
  };
  EXPECT_THAT(read_inners_a(m), IsOkAndHolds(IsEmpty()));
  m.add_inners();
  EXPECT_THAT(read_inners_a(m), IsOkAndHolds(ElementsAre(std::nullopt)));
  m.add_inners()->set_a(7);
  EXPECT_THAT(read_inners_a(m), IsOkAndHolds(ElementsAre(std::nullopt, 7)));
  m.add_inners()->set_a(37);
  EXPECT_THAT(read_inners_a(m), IsOkAndHolds(ElementsAre(std::nullopt, 7, 37)));

  // inners[:]/as[:]
  auto read_inners_as = [](const auto& m) {
    return ReadDenseArrayValue<int32_t>(
        {"inners", "as"}, {RepeatedFieldAccess{}, RepeatedFieldAccess{}}, m);
  };
  EXPECT_THAT(read_inners_as(m), IsOkAndHolds(IsEmpty()));
  m.mutable_inners(0)->add_as(0);
  m.mutable_inners(0)->add_as(57);
  m.mutable_inners(2)->add_as(19);
  m.mutable_inners(2)->add_as(3);
  m.mutable_inners(2)->add_as(17);
  EXPECT_THAT(read_inners_as(m), IsOkAndHolds(ElementsAre(0, 57, 19, 3, 17)));

  // different types
  *m.add_repeated_str() = "19";
  *m.add_repeated_str() = "17";
  EXPECT_THAT(ReadDenseArrayTopLevelValue<Text>("repeated_str", m),
              IsOkAndHolds(ElementsAre("19", "17")));
  *m.add_repeated_raw_bytes() = "17";
  *m.add_repeated_raw_bytes() = "19";
  EXPECT_THAT(ReadDenseArrayTopLevelValue<Bytes>("repeated_raw_bytes", m),
              IsOkAndHolds(ElementsAre("17", "19")));
  m.add_repeated_floats(19.0f);
  m.add_repeated_floats(17.0f);
  EXPECT_THAT(ReadDenseArrayTopLevelValue<float>("repeated_floats", m),
              IsOkAndHolds(ElementsAre(19.0f, 17.0f)));
  m.add_repeated_doubles(19.0);
  m.add_repeated_doubles(17.0);
  EXPECT_THAT(ReadDenseArrayTopLevelValue<double>("repeated_doubles", m),
              IsOkAndHolds(ElementsAre(19.0, 17.0)));
  m.add_repeated_int32s(19);
  m.add_repeated_int32s(17);
  EXPECT_THAT(ReadDenseArrayTopLevelValue<int>("repeated_int32s", m),
              IsOkAndHolds(ElementsAre(19, 17)));
  m.add_repeated_int64s(19);
  m.add_repeated_int64s(17);
  EXPECT_THAT(ReadDenseArrayTopLevelValue<int64_t>("repeated_int64s", m),
              IsOkAndHolds(ElementsAre(19, 17)));
  m.add_repeated_uint32s(19);
  m.add_repeated_uint32s(17);
  EXPECT_THAT(ReadDenseArrayTopLevelValue<int64_t>("repeated_uint32s", m),
              IsOkAndHolds(ElementsAre(19, 17)));
  m.add_repeated_uint64s(19);
  m.add_repeated_uint64s(17);
  EXPECT_THAT(ReadDenseArrayTopLevelValue<uint64_t>("repeated_uint64s", m),
              IsOkAndHolds(ElementsAre(19, 17)));
  m.add_repeated_bools(true);
  m.add_repeated_bools(false);
  EXPECT_THAT(ReadDenseArrayTopLevelValue<bool>("repeated_bools", m),
              IsOkAndHolds(ElementsAre(true, false)));
  m.add_repeated_enums(ProtoRoot::SECOND_VALUE);
  m.add_repeated_enums(ProtoRoot::DEFAULT);
  EXPECT_THAT(
      ReadDenseArrayTopLevelValue<int>("repeated_enums", m),
      IsOkAndHolds(ElementsAre(ProtoRoot::SECOND_VALUE, ProtoRoot::DEFAULT)));
}

absl::StatusOr<::arolla::DenseArray<proto::arolla_size_t>>
ReadTopLevelSizeAsArray(const std::string& field_name,
                        const google::protobuf::Message& m) {
  return ReadDenseArrayValue<proto::arolla_size_t>(
      {field_name}, {RepeatedFieldSizeAccess{}}, m);
}

absl::StatusOr<::arolla::DenseArrayShape> ReadTopLevelSizeAsShape(
    const std::string& field_name, const google::protobuf::Message& m) {
  ASSIGN_OR_RETURN(auto reader, ProtoTypeReader::CreateDenseArrayShapeReader(
                                    {kRootDescr->FindFieldByName(field_name)},
                                    {RepeatedFieldSizeAccess{}}));
  return ReadValue<::arolla::DenseArrayShape>(*reader, m);
}

TEST(ProtoTypeReader, CreateRepeatedSizeAccessReader) {
  ::testing_namespace::Root m;
  EXPECT_THAT(ReadTopLevelSizeAsArray("ys", m), IsOkAndHolds(ElementsAre(0)));
  EXPECT_THAT(ReadTopLevelSizeAsShape("ys", m),
              IsOkAndHolds(DenseArrayShape{0}));
  m.add_ys(89);
  m.add_ys(57);
  EXPECT_THAT(ReadTopLevelSizeAsArray("ys", m), IsOkAndHolds(ElementsAre(2)));
  EXPECT_THAT(ReadTopLevelSizeAsShape("ys", m),
              IsOkAndHolds(DenseArrayShape{2}));

  // inners[:]
  EXPECT_THAT(ReadTopLevelSizeAsArray("inners", m),
              IsOkAndHolds(ElementsAre(0)));
  EXPECT_THAT(ReadTopLevelSizeAsShape("inners", m),
              IsOkAndHolds(DenseArrayShape{0}));
  m.add_inners();
  EXPECT_THAT(ReadTopLevelSizeAsArray("inners", m),
              IsOkAndHolds(ElementsAre(1)));
  EXPECT_THAT(ReadTopLevelSizeAsShape("inners", m),
              IsOkAndHolds(DenseArrayShape{1}));
  m.add_inners();
  EXPECT_THAT(ReadTopLevelSizeAsArray("inners", m),
              IsOkAndHolds(ElementsAre(2)));
  EXPECT_THAT(ReadTopLevelSizeAsShape("inners", m),
              IsOkAndHolds(DenseArrayShape{2}));

  // inners[:]/as[:]
  m.clear_inners();
  auto read_inners_as_size = [](const auto& m) {
    return ReadDenseArrayValue<proto::arolla_size_t>(
        {"inners", "as"}, {RepeatedFieldAccess{}, RepeatedFieldSizeAccess{}},
        m);
  };
  EXPECT_THAT(read_inners_as_size(m), IsOkAndHolds(IsEmpty()));
  m.add_inners();
  m.mutable_inners(0)->add_as(0);
  m.mutable_inners(0)->add_as(57);
  m.add_inners();
  m.add_inners();
  m.mutable_inners(2)->add_as(19);
  m.mutable_inners(2)->add_as(3);
  m.mutable_inners(2)->add_as(17);
  EXPECT_THAT(read_inners_as_size(m), IsOkAndHolds(ElementsAre(2, 0, 3)));
}

}  // namespace
}  // namespace arolla::proto
