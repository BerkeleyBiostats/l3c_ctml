NIH Long COVID Computational Challenge -- Targeted Machine Learning Analysis Group

This is the formatted competition code for the L3C Challenge entry of the Targeted Machine Learning Analysis Group at UC Berkeley. See (TODO: maybe add writeup here for details of our analysis plan and results)

# Steps for replicating the analysis
(A). Data preparatin: 
	1. obtain the synthetic data (contact @trberg for box access) and features.csv  (contact Targeted Machine Learning Analysis Group)
	2. extract the synthetic data: `tar -xzf synthetic_data.tar.gz`
		2.1. The data sets should be stored in the `synthetic_data` folder with `training` and `testing` folders.
	3. add the additional data files to the synthetic data training folder and testing folder:
		3.1. concept.csv
		3.2. features.csv
	4. run `data_preparation.py`
		4.1. the output data will be in the `` folder
		4.2. the source files are in `src/STEP1_feature` folder
(B). Data amalysis:
	1.
	
	
	
3. build the docker container `utils/build.sh`
4. run `utils/do_analysis.sh`
5. fit models and predictions will be in the `output` folder
# Enclave code formatter

The python module format_code can process raw code exported from the enclave (as in the `src_raw` folder) and generate runnable python code (as in the `src` folder). R is not currently supported.

