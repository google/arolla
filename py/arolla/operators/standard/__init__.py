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

"""This module loads all the operator declarations."""

from arolla.operators.standard import array
from arolla.operators.standard import bitwise
from arolla.operators.standard import bool  # pylint: disable=redefined-builtin
from arolla.operators.standard import core
from arolla.operators.standard import dict  # pylint: disable=redefined-builtin
from arolla.operators.standard import edge
from arolla.operators.standard import experimental
from arolla.operators.standard import math
from arolla.operators.standard import namedtuple
from arolla.operators.standard import qtype
from arolla.operators.standard import random
from arolla.operators.standard import seq
from arolla.operators.standard import strings
