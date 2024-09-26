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
#ifndef AROLLA_CODEGEN_IO_TESTING_TEST_NATIVE_STRUCT_H_
#define AROLLA_CODEGEN_IO_TESTING_TEST_NATIVE_STRUCT_H_

#include <string>

#include "arolla/proto/testing/test.pb.h"

namespace testing_namespace {

struct InnerNativeStruct {
  int a;
};

struct RootNativeStruct {
  int x = 0;
  std::string str;
  std::string raw_bytes;
  InnerNativeStruct* inner = nullptr;
  Inner inner_proto;
};

}  // namespace testing_namespace

#endif  // AROLLA_CODEGEN_IO_TESTING_TEST_NATIVE_STRUCT_H_
