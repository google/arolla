# OpTools

OpTools is a pack of functions and decorators helping to declare and use arolla
operators.

The entry point to this package is `arolla.optools` that corresponds to the
`arolla.optools.optools` Python module.

How to declare and register a new operator `solve_quadratic_equation`:

```python
from arolla import arolla


@arolla.optools.add_to_registry('experimental.solve_quadratic_equation')
@arolla.optools.as_lambda_operator('solve_quadratic_equation')
def solve_quadratic_equation(a, b, c):
  b = b / a
  c = c / a
  d = (b * b - 4. * c)**0.5
  x0 = (-b - d) / 2.
  x1 = (-b + d) / 2.
  return arolla.M.core.make_tuple(x0, x1)
```

After that you can use the new operator either directly:

```python
solve_quadratic_equation(1.0, 0.0, -1.0)
```

or through the registry

```python
M.experimental.solve_quadratic_equation(1.0, 0.0, -1.0)
```

## Operator package

`optools.bzl` provides a build rule for an operator package declaration.

```python
# BUILD
load("//devtools/python/blaze:pytype.bzl", "pytype_strict_library")
load(
    "//py/arolla/optools:optools.bzl",
    "arolla_cc_operator_package",
    "arolla_operator_package_snapshot",
)

arolla_cc_operator_package(
    name = "solve_quadratic_equation_cc",
    snapshot = ":solve_quadratic_equation.operator_package.pb2",
)

arolla_operator_package_snapshot(
    name = "solve_quadratic_equation.operator_package.pb2",
    imports = ["arolla.optools.examples.solve_quadratic_equation"],
    deps = [":solve_quadratic_equation_py"],
)

pytype_strict_library(
    name = "solve_quadratic_equation_py",
    srcs = ["solve_quadratic_equation.py"],
    deps = ["//py/arolla"],
)
```
