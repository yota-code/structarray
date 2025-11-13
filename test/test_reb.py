#!/usr/bin/env python3

import sys

import numpy as np


import structarray

u = structarray.DataHandler(sys.argv[1])
t = u["time_ms"] / 1000.0
tf = u["_L33_MfcPflight.data.gps.position_velocity.time_of_fix"]
Vz = u["_L33_MfcPflight.data.gps.position_velocity.vertical_velocity"] / 60.0 # en ft/min -> ft/sec
Zm = u["_L33_MfcPflight.data.gps.position_velocity.msl_altitude"] # en ft

Zz = np.cumsum(Vz) * (t[1] - t[0]) + Zm[0]

import matplotlib.pyplot as plt
import mplcursors

print((1965.7 - 2005.8) / (3857.71 - 3864.82))

plt.subplot(2,1,1)
plt.plot(tf, Vz, '+--')
plt.grid()
plt.subplot(2,1,2)
y1 = plt.plot(t, Zm)
mplcursors.cursor(y1, hover=True)
y2 = plt.plot(t, Zz)
mplcursors.cursor(y2, hover=True)
plt.grid()
plt.show()