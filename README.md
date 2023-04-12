NIH Long COVID Computational Challenge -- Targeted Machine Learning Analysis Group

This is the formatted competition code for the L3C Challenge entry of the Targeted Machine Learning Analysis Group at UC Berkeley. See (TODO: maybe add writeup here for details of our analysis plan and results)

## Steps for replicating the analysis
	
The python module format_code can process raw code exported from the enclave (as in the `src_raw` folder) and generate runnable python code (as in the `src` folder). R is not currently supported.

### Set up environment
```
pip install -r requirements.txt
```

### Data Preparation Steps
The following steps are required to prepare the data for analysis:

1. Obtain the Synthetic Data

To obtain the synthetic data, please contact @trberg for access to the box where it is stored. Additionally, contact the Targeted Machine Learning Analysis Group to obtain the `features.csv` file.

2. Extract the Synthetic Data

After obtaining the synthetic data, extract it using the command: `tar -xzf synthetic_data.tar.gz`. The extracted data should be stored in the synthetic_data folder, with subfolders for training and testing.

3. Add Additional Data Files

Add the following files to the training and testing folders:

* concept.csv
* features.csv

4. Run data_preparation.py
Once the additional files have been added, run the `data_preparation.py` script to prepare the data for analysis.
