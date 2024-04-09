# Arolla Types Package

This package provides the builtin arolla types: tuple, scalars (like unit,
boolean, bytes, text, ints, floats), optionals, arrays.

The primary parts are:

*   QTypes (like `arolla.BOOLEAN`, `arolla.OPTIONAL_BOOLEAN`,
    `arolla.ARRAY_BOOLEAN`, ...)
*   Factory functions (`arolla.boolean`, `arolla.optional_boolean`,
    `arolla.array_boolean`, ...)
*   Utility functions (`arolla.is_tuple_qtype`, `arolla.is_scalar_qtype`,
    `arolla.is_optional_qtype`, ...)
*   QValue specialisations that make arolla values behave like regular Python
    types
*   Auto-boxing (`arolla.as_qvalue` for scalars, `arolla.array` for arrays,
    `arolla.tuple` for arolla tuples)

# QValue

`QValue` is a base class responsible for representing values of arolla types in
Python. It is internally based on `arolla::TypedValue` class in C++ and provides
an extension mechanism for better integration with Python — qvalue
specialisations.

Please check go/rl2-qvalue and arolla/abc/README.md for additional information.
