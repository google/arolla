# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Binary for generating .cc, .h and .deps files for compiled_expr library.

Files generated into provided by FLAGS.*_out_file locations.
Header file will contain single function FLAGS.model_name.
.deps file contains all dependencies required to compile generated library.

Model to generate code for is retrieved by calling function specified by
FLAGS.expr_location. All positional arguments `sys.argv[1:]` are passed
into the function.
"""

import os
import sys
from typing import Sequence

from absl import app
from absl import flags
from arolla.codegen import clib
from arolla.codegen import compiled_expr_lib

from arolla.codegen import utils as codegen_utils
from arolla.codegen.io import cpp

FLAGS = flags.FLAGS


def main(argv: Sequence[str]) -> None:
  if len(argv) > 1:
    raise app.UsageError('Too many command-line arguments.')
  sys.setrecursionlimit(FLAGS.recursion_limit)
  assert len(FLAGS.expr_fn) == len(FLAGS.cpp_function_name)
  exprs = []
  operator_datas = []
  for expr_fn in FLAGS.expr_fn:
    expr = codegen_utils.call_function_from_json(expr_fn)
    if not FLAGS.export_named_outputs:
      expr = compiled_expr_lib.strip_export_annotations(expr)
    exprs.append(expr)
    operator_datas.append(
        clib.generate_operator_code(
            expr, inputs_are_cheap_to_read=FLAGS.inputs_are_cheap_to_read
        )
    )

  header_template, cc_template = compiled_expr_lib.jinja_templates()

  cc_file_name = os.path.join(FLAGS.output_dir, FLAGS.cc_out_file)
  cpp_function_names = [
      cpp.CppName.from_full_name(name) for name in FLAGS.cpp_function_name
  ]
  header_guard = cpp.generate_header_guard(FLAGS.build_target)
  # we need a unique name, reusing header guard
  operator_name = header_guard
  with open(cc_file_name, 'w', encoding='utf-8') as f:
    f.write(
        cc_template.render(
            build_target=FLAGS.build_target,
            operator_name=operator_name,
            headers=sorted(
                set.union(
                    *(operator_data.headers for operator_data in operator_datas)
                )
            ),
            operator_datas=zip(cpp_function_names, operator_datas),
            LValueKind=clib.LValueKind,
            RValueKind=clib.RValueKind,
            compiled_expr_lib=compiled_expr_lib,
        )
    )

  expr_texts = [str(expr) for expr in exprs]
  h_file_name = os.path.join(FLAGS.output_dir, FLAGS.h_out_file)
  with open(h_file_name, 'w', encoding='utf-8') as f:
    f.write(
        header_template.render(
            build_target=FLAGS.build_target,
            header_guard=header_guard,
            function_name_expressions=zip(
                cpp_function_names, operator_datas, expr_texts),
            iwyu_private=FLAGS.use_iwyu_private,
        )
    )

  deps_file_name = os.path.join(FLAGS.output_dir, FLAGS.deps_out_file)
  with open(deps_file_name, 'w', encoding='utf-8') as f:
    f.write(
        '\n'.join(
            sorted(
                set.union(
                    *(operator_data.deps for operator_data in operator_datas)
                )
            )
        )
    )

  io_file_name = os.path.join(FLAGS.output_dir, FLAGS.io_out_file)
  with open(io_file_name, 'w', encoding='utf-8') as f:
    f.write(
        compiled_expr_lib.gen_model_input_output_info(
            operator_datas, build_target=FLAGS.build_target
        )
    )


if __name__ == '__main__':
  app.run(main)
