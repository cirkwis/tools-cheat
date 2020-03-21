#!/bin/sh

SCHEMAS_FILES=""

#load config files from hdfs#
for ELEMENT in $(hdfs dfs -ls ${pom.enrichedata.hdfs.conf_schemas_dir}/*.conf | awk  '{print $NF}')
do
 if [ -n "$SCHEMAS_FILES" ]; then
        SCHEMAS_FILES="${SCHEMAS_FILES},${ELEMENT}"
 else
    SCHEMAS_FILES="${ELEMENT}"
 fi
done

spark-submit --class com.socgen.bsc.cbs.businessviews.thirdParties.Main --master yarn \
        --deploy-mode cluster \
        --num-executors ${pom.businessviews.bt.num-executors} --executor-cores ${pom.businessviews.bt.executor-cores} --executor-memory ${pom.businessviews.bt.executor-memory} \
        --queue ${pom.businessviews.yarnqueue} \
        --jars ${SCHEMAS_FILES} ${pom.businessviews.hdfs.lib_dir}/${pom.businessviews.spark_jar_name} buildPP
