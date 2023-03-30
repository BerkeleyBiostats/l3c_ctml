import re
def format_raw(path):
	# read and split the text into lines
	file = open(path)
	is_py = ".py" in path
	text = file.read()
	lines = text.split("\n")
	line_nums = range(len(lines))

	# TODO: maybe a different format
	tp_output = open("scratch/data_deps.txt","a")
	# separate the transform_pandas decorators and the sql statements
	# TODO: don't assume that these are the same
	# end_tp_lines could be part of SQL statement
	start_tp_lines = [l for l in line_nums if "transform_pandas" in lines[l]]
	end_tp_lines = [l for l in line_nums if lines[l]==")"]

	statement_nums = range(len(start_tp_lines))

	# extract the transform_pandas decorator
	def get_tp_statement(statement_num):
		start = start_tp_lines[statement_num]
		end = end_tp_lines[statement_num]
		statement_lines = lines[start:(end + 1)]
		statement = '\n'.join(statement_lines)
		tp_output.write(statement)
		tp_output.write("\n")
		return(statement)

	# extract the statement (ie not the decorator)
	def get_statement(statement_num):
		start = end_tp_lines[statement_num] + 1
		if(statement_num < len(start_tp_lines) - 1):
			end = start_tp_lines[statement_num + 1]
		else:
			end = len(lines)
		statement_lines = lines[start:end]
		statement = '\n'.join(statement_lines)

		return(statement)


	def sql_statement_to_py_fun(statement_num):
		py_fun_proto = """%s
	def %s():
	      statement = '''%s\'''
	      return(statement)\n"""
		tp_statement = get_tp_statement(statement_num)
		fun_name = "sql_statement_%02i" % statement_num
		# TODO: fix escaping
		sql_statement = re.escape(get_statement(statement_num))
		py_fun = py_fun_proto % (tp_statement, fun_name, sql_statement)

		return((fun_name, py_fun))

	def process_py_statement(statement_num):
		tp_statement = get_tp_statement(statement_num)
		py_statement = get_statement(statement_num)
		return (tp_statement + "\n" + py_statement)


	# this should return the original file, with the transform_pandas data output to csv
	if is_py:
		all_states = [process_py_statement(s) for s in statement_nums]
		pre_states = lines[0:start_tp_lines[0]]
		formatted = '\n'.join(pre_states) + '\n' + '\n'.join(all_states)

	else:
		all_funs = [sql_statement_to_py_fun(s) for s in statement_nums]
		(fun_names, fun_strs) = zip(*all_funs)

		all_str = '__all__ = [%s]' % (','.join(fun_names))
		formatted = '\n'.join(list(fun_strs) + [all_str])
	tp_output.close()
	return(formatted)
	# TODO: returnformatted string, let outer function save it
	#formatted_file = open('l3c/scratch/formatted_sql_test.py',"w")
	#formatted_file.write(formatted)