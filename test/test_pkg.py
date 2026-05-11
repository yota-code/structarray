#!/usr/bin/env python3

import os
from cc_pathlib import Path
import structarray
import csv
import numpy as np

test_files = [
	#"dummy_data/rec_from_agent.hdf5",
	"dummy_data/rec_from_agent.reb", #need context_map
	"dummy_data/rec_from_agent.rez",
	"dummy_data/rec_from_mirror.csv",
	"dummy_data/rec_from_rise.csv"
]



if __name__ == '__main__' :
	for test in test_files:
		data_pth = Path(test)
		sa = structarray.open(data_pth, None)
		print(test ," : ", sa.meta.search('time')[0])
