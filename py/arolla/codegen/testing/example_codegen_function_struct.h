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
#ifndef THIRD_PARTY_PY_AROLLA_CODEGEN_TESTING_EXAMPLE_CODEGEN_FUNCTION_STRUCT_H_
#define THIRD_PARTY_PY_AROLLA_CODEGEN_TESTING_EXAMPLE_CODEGEN_FUNCTION_STRUCT_H_

#include <string>
#include <vector>

namespace test_namespace {

// Struct version of test_namespace.FooInput proto.
struct FooInputStruct {
  float a;
  std::string string_field;
  float unused_field;

  // Struct version of test_namespace.FooInput.BarInput proto.
  struct BarInputStruct {
    float c;
    std::vector<FooInputStruct> nested_foo;
    float unused_field;
  };
};

// Struct version of test_namespace.ScoringOutput proto.
struct ScoringOutputStruct {
  float result;
  float unused_field;
};

}  // namespace test_namespace

#endif  // THIRD_PARTY_PY_AROLLA_CODEGEN_TESTING_EXAMPLE_CODEGEN_FUNCTION_STRUCT_H_
