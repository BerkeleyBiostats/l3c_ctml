import os
from cleanup_export.format_raw import format_raw
from pathlib import Path

#def format_all():
raw_folder = "src_raw/STEP1_feature/"
dest_folder = "src/STEP1_feature/"
min_size=1000

all_files = []
for path, subdirs, files in os.walk(raw_folder):
    for name in files:
        full_file = (os.path.join(path, name))
        if os.path.getsize(full_file)>min_size:
            all_files.append(full_file)

for file in all_files:
        print("formatting " + file)
        formatted = format_raw(file)
        # make sure dest folder exists, then write
        formatted_path = file.replace(raw_folder, dest_folder)
        formatted_path = formatted_path.replace(".sql", "_sql.py")
        # TODO: pad numbers so these are loadable python modules
        Path(os.path.dirname(formatted_path)).mkdir(parents=True, exist_ok=True)
        formatted_file = open(formatted_path, 'w')
        formatted_file.write(formatted)

