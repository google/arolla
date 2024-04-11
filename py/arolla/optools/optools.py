# Copyright 2024 Google LLC
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

"""Module for operator manipulation tooling."""

from arolla.optools import decorators as _decorators
from arolla.optools import helpers as _helpers
from arolla.optools.constraints import constraints as _constraints

add_to_registry = _decorators.add_to_registry
add_to_registry_as_overload = _decorators.add_to_registry_as_overload
add_to_registry_as_overloadable = _decorators.add_to_registry_as_overloadable
as_backend_operator = _decorators.as_backend_operator
as_lambda_operator = _decorators.as_lambda_operator
as_py_function_operator = _decorators.as_py_function_operator
dispatch = _decorators.dispatch

suppress_unused_parameter_warning = _helpers.suppress_unused_parameter_warning

constraints = _constraints
