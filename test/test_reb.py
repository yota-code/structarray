#!/usr/bin/env python3

import sys

import structarray

u = structarray.DataHandler(sys.argv[1])
y = u["_C10_MfcPflight_Gfx._L724_MfcPflight.position_velocity.vertical_velocity"]

import matplotlib.pyplot as plt

plt.plot(y)
plt.grid()
plt.show()