#!/usr/bin/env python3

from cc_pathlib import Path

import structarray

for name in ["input", "output", "C_"] :
    u = structarray.StructInfo(Path("main.exe"))

