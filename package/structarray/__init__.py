#!/usr/bin/env python3

import os

from cc_pathlib import Path

def open(data:Path|str, meta:Path=None) :

	data = Path(data).resolve()

	match data.suffix :
		case '.reb' :
			import structarray.rebin

			if "STRUCTARRAY_mapping_DIR" in os.environ :
				mapping_dir = Path(os.environ["STRUCTARRAY_mapping_DIR"])

				p_lst = data.resolve().parts
				d_ident = p_lst[-2]
				d_struct = p_lst[-3]

				meta_pth = mapping_dir / d_struct / f"context_map.{d_ident}.tsv"

				if meta_pth.is_file() :
					meta = meta_pth

			return structarray.rebin.DataRebin(data, meta)
		case '.rez' :
			import structarray.rezip
			return structarray.rezip.DataRezip(data)
		case '.csv' :
			import structarray.recsv
			return structarray.recsv.DataReCsv(data)
		case _ :
			raise NotImplementedError(f"Unknown file type: {data.suffix}")
	