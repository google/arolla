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
// A demo application that deserializes and prints values and expressions from
// a pb file.
//
// It demonstrates how use deserialization, and also used for
// the deserialization binary size monitoring.

#include <cstddef>
#include <fstream>
#include <iostream>
#include <vector>

#include "absl/flags/parse.h"
#include "absl/log/check.h"
#include "absl/log/log.h"
#include "absl/status/statusor.h"
#include "arolla/expr/eval/invoke.h"
#include "arolla/expr/expr_debug_string.h"
#include "arolla/serialization/decode.h"
#include "arolla/serialization_base/base.pb.h"
#include "arolla/util/init_arolla.h"

using ::arolla::InitArolla;
using ::arolla::serialization::Decode;
using ::arolla::serialization::DecodingOptions;
using ::arolla::serialization_base::ContainerProto;

int main(int argc, char** argv) {
  const std::vector<char*> filenames = absl::ParseCommandLine(argc, argv);
  if (filenames.size() == 1) {
    LOG(FATAL) << "Usage: file_expr.pb ...";
  }
  InitArolla();
  for (size_t i = 1; i < filenames.size(); ++i) {
    const char* const filename = filenames[i];
    std::ifstream istream(filename, std::ifstream::in | std::ifstream::binary);
    if (!istream) {
      LOG(FATAL) << "Unable to open: " << filename;
    }

    ContainerProto container_proto;
    if (!container_proto.ParseFromIstream(&istream)) {
      LOG(FATAL) << "Unable to parse: " << filename;
    }

    const auto decode_result =
        Decode(container_proto,
               DecodingOptions{.infer_attributes_for_operator_nodes = false});
    if (!decode_result.ok()) {
      LOG(FATAL) << "Unable to parse: " << filename << ";\n"
                 << decode_result.status();
    }
    for (const auto& value : decode_result->values) {
      std::cout << value.Repr() << '\n';
    }
    for (const auto& expr : decode_result->exprs) {
      std::cout << ToDebugString(expr) << " : ";
      // NOTE: The following line adds a dependency on the evaluation engine.
      //       => it affect the weightwatcher result.
      auto value = ::arolla::expr::Invoke(expr, {});
      if (; value.ok()) {
        std::cout << value->Repr() << '\n';
      } else {
        std::cout << value.status() << '\n';
      }
    }
  }
  return 0;
}
