#!/usr/bin/env python3

import os
import subprocess
import sys

from cc_pathlib import Path

import structarray

ctype_info_template = '''#include <stdlib.h>
#include <stdio.h>

#ifndef _INCLUDE_SCADE_TYPES
	#include "scade/scade_types.h"
#endif

#define inttype_info(t) printf("\\t\\"%s\\" : \\"%s%d\\",\\n", #t, ( ( (((t)(0)) - 1) > 0) ? ("N") : ("Z")), sizeof(t))
#define realtype_info(t) printf("\\t\\"%s\\" : \\"%s%d\\",\\n", #t, "R", sizeof(t))
#define pointertype_info(t) printf("\\t\\"%s\\" : \\"%s%d\\",\\n", #t, "P", sizeof(t))

int main(int argc, char * argv[]) {

	printf("{\\n");

	pointertype_info(void*);

	inttype_info(unsigned int);
	inttype_info(bool);
	inttype_info(signed char);
	inttype_info(unsigned char);
	inttype_info(short);
	inttype_info(int);
	inttype_info(long);
	inttype_info(long long);
	inttype_info(unsigned long);

	realtype_info(float);
	realtype_info(double);

	printf("}\\n");

	return EXIT_SUCCESS; 

}
'''

def get_ctype_info(cwd) :
	src_name = 'structarray_ctype_info.c'
	exe_name = 'structarray_ctype_info.exe'

	include_lst = [ "../include/scade", "../include/fctext", "../include" ]

	cwd.make_dirs()
	(cwd / src_name).write_text(ctype_info_template)

	cmd = (
		["gcc", "-save-temps", "-std=c99", "-g"] +
		[f"-I{include_dir}" for include_dir in include_lst] +
		[src_name, "-o", exe_name]
	)
	ret = cwd.run(* cmd)
	if ret.returncode != 0 :
		raise ValueError("gcc couldn't compile properly")

	ret = cwd.run(f"./{exe_name}")
	txt = ret.stdout.decode(sys.stdout.encoding)

	(cwd / "structarray_ctype.json").write_text(txt.replace(',\n}', '\n}'))

scade_context_template = '''#include <stdlib.h>
#include <stdio.h>

#include <string.h>

#ifndef _INCLUDE_SCADE_TYPES
	#include "scade/scade_types.h"
#endif

#ifndef _INCLUDE_upmv_loop
	#include "scade/{name}.h"
#endif

_C_{name} context = {{0}};

int main(int argc, char * argv[]) {{

	memset(& context, 0, sizeof(_C_{name}));

	return EXIT_SUCCESS; 

}}
'''

def get_scade_context(cwd, name) :
	src_name = 'structarray_scade_context.c'
	exe_name = 'structarray_scade_context.exe'

	include_lst = [ "../include/scade", "../include/fctext", "../include" ]

	cwd.make_dirs()
	(cwd / src_name).write_text(scade_context_template.format(name=name))

	cmd = (
		["gcc", "-save-temps", "-std=c99", "-g"] +
		[f"-I{include_dir}" for include_dir in include_lst] +
		[src_name, "-o", exe_name]
	)

	ret = cwd.run(* cmd)
	if ret.returncode != 0 :
		raise ValueError("gcc couldn't compile properly")

	u = structarray.StructInfo(cwd / exe_name)
	u.parse("context")
	u.save(cwd / f"context.tsv")

	return u


scade_interface_template = '''#include <stdlib.h>
#include <stdio.h>

#include <string.h>

#ifndef _INCLUDE_SCADE_TYPES
	#include "scade/scade_types.h"
#endif

#include "unitest/interface.h"

unitest_input_T input = {0};
unitest_output_T output = {0};

int main(int argc, char * argv[]) {

	memset(& input, 0, sizeof(unitest_input_T));
	memset(& output, 0, sizeof(unitest_output_T));

	return EXIT_SUCCESS;
}
'''

def get_scade_interface(cwd, ) :
	src_name = 'structarray_scade_interface.c'
	exe_name = 'structarray_scade_interface.exe'

	include_lst = [ "../include/scade", "../include/fctext", "../include/unitest", "../include" ]

	cwd.make_dirs()
	(cwd / src_name).write_text(scade_interface_template)

	cmd = (
		["gcc", "-save-temps", "-std=c99", "-g"] +
		[f"-I{include_dir}" for include_dir in include_lst] +
		[src_name, "-o", exe_name]
	)
	ret = cwd.run(* cmd)
	if ret.returncode != 0 :
		raise ValueError("gcc couldn't compile properly")

	for k in ['input', 'output'] :
		u = structarray.StructInfo(cwd / exe_name)
		u.parse(k)
		u.save(cwd / f"{k}.tsv")