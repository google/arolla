# Codegeneration

This directory contains following utilities
* `io/io.bzl` codegeneration for `InputLoader` and `SlotListener`
* `expr/...` directory with C++ utilities for `Expr` codegeneration
  User facing codegeneration is implemented in
  `py/arolla/expr/codegen/expr.bzl`
* `qexpr/...` utilities for defining backend level operators
* `operator_package/...` utilities to define `Expr` level collection
  of operators embedded into the binary.
* `dict/...` extension for `dict` literal support in `Expr` codegeneration
