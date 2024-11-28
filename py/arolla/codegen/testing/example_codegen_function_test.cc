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
#include "py/arolla/codegen/testing/example_codegen_function.h"

#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "py/arolla/codegen/testing/_example_codegen_function_input_loaders.h"
#include "py/arolla/codegen/testing/_example_codegen_function_slot_listeners.h"
#include "py/arolla/codegen/testing/example_codegen_function.pb.h"
#include "py/arolla/codegen/testing/example_codegen_function_extensions.pb.h"
#include "py/arolla/codegen/testing/example_codegen_function_struct.h"

namespace test_namespace {
namespace {

using ::testing::ElementsAre;
using ::testing::Field;
using ::testing::FloatEq;
using ::testing::IsFalse;
using ::testing::IsNull;
using ::testing::NotNull;
using ::testing::Property;

namespace generated =
 ::py_arolla_codegen_testing_example_codegen_function;

TEST(E2eFunctionTest, FirstFunction) {
  FooInput scalar_input;
  scalar_input.set_a(1.2);
  scalar_input.set_string_field("Five!");

  std::vector<FooInput> array_input_1;
  array_input_1.emplace_back().set_a(1.);
  array_input_1.emplace_back().set_a(2.);
  array_input_1.emplace_back().set_a(3.);

  std::vector<FooInput::BarInput> array_input_2;
  array_input_2.emplace_back().set_c(4.);
  array_input_2.back().add_nested_foo()->set_a(7.);
  array_input_2.back().add_nested_foo()->set_a(8.);
  array_input_2.back().add_nested_foo()->set_a(9.);
  array_input_2.back()
      .MutableExtension(FooExtension::extension_foo)
      ->mutable_foo()
      ->set_a(11.);
  array_input_2.emplace_back().set_c(5.);
  // No nested_foo().a().
  array_input_2.back()
      .MutableExtension(FooExtension::extension_foo)
      ->mutable_foo()
      ->set_a(12.);
  array_input_2.emplace_back().set_c(6.);
  array_input_2.back().add_nested_foo()->set_a(10.);
  array_input_2.back()
      .MutableExtension(FooExtension::extension_foo)
      ->mutable_foo()
      ->set_a(13.);
  Output scalar_output;
  std::vector<Output> array_output_1;
  std::vector<Output> array_output_2;
  ASSERT_OK(FirstFunction(scalar_input, array_input_1, array_input_2,
                          scalar_output, array_output_1, array_output_2));
  // scalar level output.
  EXPECT_THAT(scalar_output, Property(&Output::result, FloatEq(8.2)));
  // /arrays level output.
  EXPECT_THAT(array_output_1,
              ElementsAre(Property(&Output::result, FloatEq(24.)),
                          Property(&Output::has_result, IsFalse()),
                          Property(&Output::result, FloatEq(32.))));
  // /arrays/nested_foo level output.
  EXPECT_THAT(array_output_2,
              ElementsAre(Property(&Output::result, FloatEq(8.)),
                          Property(&Output::result, FloatEq(9.)),
                          Property(&Output::result, FloatEq(10.)),
                          Property(&Output::result, FloatEq(13.))));
}

TEST(E2eFunctionTest, FirstFunctionOnStruct) {
  FooInputStruct scalar_input{.a = 1.2, .string_field = "Five!"};

  std::vector<FooInput> array_input_1;
  array_input_1.emplace_back().set_a(1.);
  array_input_1.emplace_back().set_a(2.);
  array_input_1.emplace_back().set_a(3.);

  std::vector<FooInput::BarInput> array_input_2;
  array_input_2.emplace_back().set_c(4.);
  array_input_2.back().add_nested_foo()->set_a(7.);
  array_input_2.back().add_nested_foo()->set_a(8.);
  array_input_2.back().add_nested_foo()->set_a(9.);
  array_input_2.back()
      .MutableExtension(FooExtension::extension_foo)
      ->mutable_foo()
      ->set_a(11.);
  array_input_2.emplace_back().set_c(5.);
  // No nested_foo().a().
  array_input_2.back()
      .MutableExtension(FooExtension::extension_foo)
      ->mutable_foo()
      ->set_a(12.);
  array_input_2.emplace_back().set_c(6.);
  array_input_2.back().add_nested_foo()->set_a(10.);
  array_input_2.back()
      .MutableExtension(FooExtension::extension_foo)
      ->mutable_foo()
      ->set_a(13.);
  Output scalar_output;
  std::vector<ScoringOutputStruct> array_output_1;
  std::vector<Output> array_output_2;
  ASSERT_OK(FirstFunctionOnStruct(scalar_input, array_input_1, array_input_2,
                                  scalar_output, array_output_1,
                                  array_output_2));
  // scalar level output.
  EXPECT_THAT(scalar_output, Property(&Output::result, FloatEq(8.2)));
  // /arrays level output.
  EXPECT_THAT(array_output_1,
              ElementsAre(Field(&ScoringOutputStruct::result, FloatEq(24.)),
                          Field(&ScoringOutputStruct::result, FloatEq(0.)),
                          Field(&ScoringOutputStruct::result, FloatEq(32.))));
  // /arrays/nested_foo level output.
  EXPECT_THAT(array_output_2,
              ElementsAre(Property(&Output::result, FloatEq(8.)),
                          Property(&Output::result, FloatEq(9.)),
                          Property(&Output::result, FloatEq(10.)),
                          Property(&Output::result, FloatEq(13.))));
}

TEST(E2eFunctionTest, SecondFunction) {
  FooInput scalar_input;
  scalar_input.set_a(1.2);
  scalar_input.set_string_field("Five!");

  std::vector<FooInput> array_input;
  array_input.emplace_back().set_a(1.);
  array_input.emplace_back().set_a(2.);
  array_input.emplace_back().set_a(3.);
  array_input.emplace_back().set_a(4.);

  Output scalar_output;
  std::vector<Output> array_output;
  ASSERT_OK(
      SecondFunction(scalar_input, array_input, scalar_output, array_output));
  EXPECT_THAT(scalar_output.GetExtension(OutputExtension::extension_output)
                  .extra_result(),
              FloatEq(3.7));
  EXPECT_THAT(array_output,
              ElementsAre(Property(&Output::result, FloatEq(8.5)),
                          Property(&Output::result, FloatEq(9.5)),
                          Property(&Output::result, FloatEq(10.5)),
                          Property(&Output::result, FloatEq(11.5))));
}

TEST(E2eFunctionTest, FooInput_InputLoader) {
  EXPECT_THAT(generated::FooInput_InputLoader()->GetQTypeOf("/a"), NotNull());
  EXPECT_THAT(generated::FooInput_InputLoader()->GetQTypeOf("/string_field"),
              NotNull());
  EXPECT_THAT(generated::FooInput_InputLoader()->GetQTypeOf("/unused_field"),
              IsNull());
}

TEST(E2eFunctionTest, FooInput_BarInput_Repeated_InputLoader) {
  EXPECT_THAT(
      generated::FooInput_BarInput_Repeated_InputLoader()->GetQTypeOf("/c"),
      NotNull());
  EXPECT_THAT(generated::FooInput_BarInput_Repeated_InputLoader()->GetQTypeOf(
                  "/nested_foo/a"),
              NotNull());
  EXPECT_THAT(generated::FooInput_BarInput_Repeated_InputLoader()->GetQTypeOf(
                  "/Ext::test_namespace.FooExtension.extension_foo/foo/a"),
              NotNull());
  EXPECT_THAT(generated::FooInput_BarInput_Repeated_InputLoader()->GetQTypeOf(
                  "/nested_foo/b"),
              IsNull());
  EXPECT_THAT(generated::FooInput_BarInput_Repeated_InputLoader()->GetQTypeOf(
                  "/unused_field"),
              IsNull());
}

TEST(E2eFunctionTest, Output_SlotListener) {
  EXPECT_THAT(generated::Output_SlotListener()->GetQTypeOf(
                  "/Ext::test_namespace.OutputExtension.extension_"
                  "output/extra_result"),
              NotNull());
  EXPECT_THAT(generated::Output_SlotListener()->GetQTypeOf("/result"),
              NotNull());
  EXPECT_THAT(generated::Output_SlotListener()->GetQTypeOf("/unused_field"),
              IsNull());
}

TEST(E2eFunctionTest, Output_Repeated_SlotListener) {
  EXPECT_THAT(generated::Output_Repeated_SlotListener()->GetQTypeOf("/result"),
              NotNull());
  EXPECT_THAT(
      generated::Output_Repeated_SlotListener()->GetQTypeOf("/unused_field"),
      IsNull());
}

}  // namespace
}  // namespace test_namespace
