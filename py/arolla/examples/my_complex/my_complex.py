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

"""Example of a custom QType definition in Arolla."""

from __future__ import annotations

from arolla import arolla
from arolla.examples.my_complex import my_complex_clib as _


class MyComplex(arolla.QValue):
  """QValue specialization for the MY_COMPLEX qtype."""

  # Note that we define __new__ instead of __init__. arolla.abc.invoke_op will
  # return a MY_COMPLEX QValue, pick the proper specialization (MyComplex) and
  # wrap it automatically.
  def __new__(cls, re: float, im: float) -> MyComplex:
    """Constructs MyComplex from the given parts."""
    return arolla.abc.invoke_op(
        'my_complex.make', (arolla.float64(re), arolla.float64(im))
    )

  @property
  def re(self) -> float:
    """Real part."""
    return float(arolla.abc.invoke_op('my_complex.get_re', (self,)))

  @property
  def im(self) -> float:
    """Imaginary part."""
    return float(arolla.abc.invoke_op('my_complex.get_im', (self,)))


MY_COMPLEX = MyComplex(0, 0).qtype


arolla.abc.register_qvalue_specialization(MY_COMPLEX, MyComplex)
