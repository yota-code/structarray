#!/usr/bin/env python3


def expand_name_gen() :
	# validated
	# undo the compaction and give back the original names
	r = yield None
	while True :
		if '/' in r :
			c, sep, z = r.partition('/')
			n_lst = p_lst[:int(c)] + z.split('.')
			# print(">>>", r, True, p_lst, c, z, n_lst)
			r = yield '.'.join(n_lst)
		else :
			# print(">>>", r, False)
			n_lst = r.split('.')
			r = yield r
		p_lst = n_lst

r_lst = """_C_upmv_app
_I0_input
_I1_input_dbg
_O0_output.pub_UpmvOut.objective.vx.is_requested
4/cmd
4/der
3/vy.is_requested
4/cmd
4/der
3/vz.is_requested""".splitlines()


exp = expand_name_gen()
next(exp)
for r in r_lst :
	v = exp.send(r)
	print(v)