#!/usr/bin/env python3

import os
import subprocess
import sys

from cc_pathlib import Path

import structarray

main_template = '''#include <stdlib.h>
#include <stdio.h>

#include <string.h>

#define inttype_info(t) printf("\\t\\"%s\\" : \\"%s%d\\",\\n", #t, ( ( (((t)(0)) - 1) > 0) ? ("N") : ("Z")), sizeof(t))
#define realtype_info(t) printf("\\t\\"%s\\" : \\"%s%d\\",\\n", #t, "R", sizeof(t))
#define pointertype_info(t) printf("\\t\\"%s\\" : \\"%s%d\\",\\n", #t, "P", sizeof(t))

#include "scade_types.h"

#include "{name}.h"

_C_{name} context = {{0}};

int main(int argc, char * argv[]) {{

	printf("{{\\n");
	pointertype_info(void*);
	inttype_info(unsigned int);
	inttype_info(bool);
	inttype_info(char);
	inttype_info(short);
	inttype_info(int);
	inttype_info(long);
	inttype_info(long long);
	realtype_info(float);
	realtype_info(double);
	printf("}}\\n");

	memset(& context, 0, sizeof(_C_{name}));

	return EXIT_SUCCESS;

}}
'''

def build_scade_map(name) :
	model_dir = Path(os.environ['STRUCTARRAY_build_DIR']) / name

	if not model_dir.is_dir() :
		raise FileNotFoundError(f"The directory does not exists: {model_dir}")
	model_pth = model_dir / f'{name}.h'
	if not model_pth.is_file() :
		raise FileNotFoundError(f"The file does not exists: {model_pth}")

	(model_dir / 'structarray_main.c').write_text(main_template.format(name=name))

	cmd = ["gcc", "-save-temps", "-g", "-I../../include/fctext", "structarray_main.c", "-o", "structarray_main.exe"]
	print(' '.join(cmd))

	ret = subprocess.run(cmd, cwd=model_dir)
	if ret.returncode != 0 :
		raise ValueError("gcc couldn't compile properly")

	ret = subprocess.run(["./structarray_main.exe",], stdout=subprocess.PIPE, cwd=model_dir)
	txt = ret.stdout.decode(sys.stdout.encoding)
	(model_dir / "structarray_type.json").write_text(txt.replace(',\n}', '\n}'))

	u = structarray.StructInfo(model_dir / 'structarray_main.exe')
	u.parse(f'_C_{name}', 'context')

if __name__ == '__main__' :
	build_scade_map(sys.argv[1])
	





