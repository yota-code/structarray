#!/usr/bin/env python3

import sys

import numpy as np

import structarray.rebin

u = structarray.rebin.DataRebin(sys.argv[1], "data/compact_map.tsv")
t = u["time_ms"] / 1000.0
tf = u["_L33_MfcPflight.data.gps.position_velocity.time_of_fix"]
Vz = u["_L33_MfcPflight.data.gps.position_velocity.vertical_velocity"] / 60.0 # en ft/min -> ft/sec
Zm = u["_L33_MfcPflight.data.gps.position_velocity.msl_altitude"] # en ft

Zz = np.cumsum(Vz) * (t[1] - t[0]) + Zm[0]

import matplotlib.pyplot as plt
import mplcursors

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