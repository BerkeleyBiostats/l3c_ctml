import re
import pandas as pd
import numpy as np
file = open("scratch/data_deps.txt")
text = file.read()
lines = text.split("\n")
output_lines = [line for line in lines if "Output" in line]
output_parsed = [(re.search('rid="(.*)"', line).group(1), None, True)\
	for line in output_lines]

input_lines = [line for line in lines if "Input" in line]
input_parsed = [(re.search('rid="(.*)"', line).group(1),\
	re.search('\s*([^=\s]*)=', line).group(1), False)\
	for line in input_lines]

all_parsed = output_parsed + input_parsed

df = pd.DataFrame(all_parsed, columns=["rid", "name", "output"])

def find_var_names(data):
	obs_names = [name for name in data['name'] if name]	
	obs_names = np.unique(obs_names)
	print(obs_names)
	if(len(obs_names)==0):
		obs_names = [None]
	output = any(data['output'])
	return(pd.DataFrame({'name': obs_names, 'output': output}))

var_names = df.groupby('rid').apply(find_var_names)
var_names = var_names.reset_index()