NIH Long COVID Computational Challenge -- Targeted Machine Learning Analysis Group

This is the formatted competition code for the L3C Challenge entry of the Targeted Machine Learning Analysis Group at UC Berkeley. See (TODO: maybe add writeup here for details of our analysis plan and results)

# Steps for replicating the analysis
1. obtain the synthetic data (contact @trberg for box access)
2. extract the synthetic data: `tar -xzf synthetic_data.tar.gz`
3. add the additional data files to the synthetic data folder:
	```concept_set_members.csv
 	   LL_concept_sets_fusion_everyone.csv
	   LL_DO_NOT_DELETE_REQUIRED_concept_sets_all.csv
	```
3. build the docker container `utils/build.sh`
4. run `utils/do_analysis.sh`
5. fit models and predictions will be in the `output` folder
# Enclave code formatter

The python module format_code can process raw code exported from the enclave (as in the `src_raw` folder) and generate runnable python code (as in the `src` folder). R is not currently supported.

