#!/usr/bin/env python3

import os
import subprocess
import sys

from cc_pathlib import Path

import structarray

scade_context_template = '''#include <stdlib.h>
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
	inttype_info(unsigned char);
	inttype_info(short);
	inttype_info(int);
	inttype_info(long);
	inttype_info(long long);
	inttype_info(unsigned long);

	realtype_info(float);
	realtype_info(double);

	printf("}}\\n");

	memset(& context, 0, sizeof(_C_{name}));

	return EXIT_SUCCESS;

}}
'''

def scade_map_context(cwd, name, include_lst) :

	cwd.make_dirs()

	(cwd / 'structarray_context.c').write_text(scade_context_template.format(name=name))

	cmd = (
		["gcc", "-save-temps", "-std=c99", "-g"] +
		[f"-I{include_dir}" for include_dir in include_lst] +
		["structarray_context.c", "-o", "structarray_context.exe"]
	)
	ret = cwd.run(* cmd)
	# ret = subprocess.run(cmd, cwd=model_dir)
	if ret.returncode != 0 :
		raise ValueError("gcc couldn't compile properly")

	ret = cwd.run("./structarray_context.exe")
	txt = ret.stdout.decode(sys.stdout.encoding)

	(cwd / "structarray_ctype.json").write_text(txt.replace(',\n}', '\n}'))

	u = structarray.StructInfo(cwd / 'structarray_context.exe')
	u.parse('context')
	u.save(cwd)

	return u
