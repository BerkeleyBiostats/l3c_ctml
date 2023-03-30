import re
import pandas as pd

file = open("scratch/data_deps.txt")
text = file.read()
lines = text.split("\n")
output_lines = [line for line in lines if "Output" in line]
output_parsed = [(re.search('rid="(.*)"', line).group(1), None)\
	for line in output_lines]

input_lines = [line for line in lines if "Input" in line]
input_parsed = [(re.search('rid="(.*)"', input_line).group(1),\
	re.search('\s*([^=\s]*)=', line).group(1))\
	for line in input_lines]

all_parsed = output_parsed + input_parsed

pd.DataFrame(all_parsed, columns=["rid", "name"])