#def format_all():
import os
folder = "src/STEP1_feature/"

for path, subdirs, files in os.walk(folder):
    for name in files:
        full_file = (os.path.join(path, name))

        # read the original file into a list of strings
        with open(full_file, "r", errors='ignore') as f:
            lines = f.readlines()

        # iterate over each line and comment out any containing "@transform_pandas"
        for i, line in enumerate(lines):
            if "@transform_pandas" in line or "ri." in line or line[0] == ")":
                lines[i] = "#" + line
            if "def sql_" in line:
                lines[i] = line.lstrip()

        with open(full_file, "w") as f:
            f.writelines(lines)