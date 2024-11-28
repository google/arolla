# Model codegeneration process

1.  Create a python library with a function returning an Arolla Expression.

    All leaves in the returned Arolla Expression must be type annotated.

    E.g.,

    ```python
    def my_expression():
      return my_a(...) + my_b(...)
    ```

1.  Define `compiled_expr` target without any deps. Provide the python library
    containing your expression function as `tools_deps`, the location of the
    function returning the Expr, and the desired name for the resulting
    codegenerated model. E.g.,

    ```
    load("//arolla/codegen:utils.bzl", "call_python_function")
    load("//py/arolla/codegen:expr.bzl", "compiled_expr")

    compiled_expr(
        name = "my_expr",
        cpp_function_name = "::my_namespace::GetMyCompiledExpr",
        model = call_python_function(
          "my_dir.expressions.my_expression",
          args = [],
          deps = ["//my_dir:expressions"],
        ),
    )
    ```

1.  Build the library with `bazel build //my_expr_dir:my_expr` and start using
    it.

    We recommend using `BindModelExecutor` method. The `InputLoader` object
    passed to the call can be the same
    input loader that is used for
    dynamic eval.

    ```
    ASSIGN_OR_RETURN(auto executor, BindModelExecutor<OutputT>(
        ::my_namespace::GetCompiledExpr(), *input_loader));

    ASSIGN_OR_RETURN(auto output, executor.Execute(input));
    ```

1.  [Optional] For troubleshooting or if you're just curious, you can take a
    look at the generated C++ files.

    ```
    bazel build //my_expr_dir:my_expr.cc //my_expr_dir:my_expr.h
    ```
