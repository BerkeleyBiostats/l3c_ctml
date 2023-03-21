FROM jupyter/pyspark-notebook

# for some reason pyspark not showing up in ipython (just jupyter)
USER jovyan

# TODO: we shouldn't need to do this
RUN pip3 install "py4j==0.10.9.5"
RUN ln -s /usr/local/spark-3.3.2-bin-hadoop3/python/pyspark /opt/conda/lib/python3.10/site-packages/pyspark


#TODO: other dependencies we load

# need to do this for jupyter for some reason
USER root
RUN fix-permissions "${CONDA_DIR}"
RUN fix-permissions "/home/${NB_USER}"

# put test files in home director
USER jovyan

# put needed files in home directory (also will mount whole project as volume)
USER jovyan

# RUN ln -s /usr/local/spark/python/lib/* /opt/

ADD run.sh /home/jovyan/run.sh

# run i3c code
# CMD ["run.sh"]