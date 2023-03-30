FROM jupyter/pyspark-notebook

# for some reason pyspark not showing up in ipython (just jupyter)
USER jovyan

# TODO: we shouldn't need to do this
RUN pip3 install "py4j==0.10.9.5"
RUN ln -s /usr/local/spark-3.3.2-bin-hadoop3/python/pyspark /opt/conda/lib/python3.10/site-packages/pyspark
RUN pip3 install shap

#TODO: other dependencies we load

# need to do this for jupyter for some reason
USER root
RUN fix-permissions "${CONDA_DIR}"
RUN fix-permissions "/home/${NB_USER}"

# put needed files in home directory (also will mount whole project as volume at runtime)

USER jovyan

ADD /utils/run.sh /home/jovyan/run.sh

CMD ["run.sh"]