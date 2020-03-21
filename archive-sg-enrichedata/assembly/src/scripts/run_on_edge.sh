#!/bin/sh
currentDir=$(dirname "$(readlink -f "$0")")

SCHEMAS_FILES=""

function usage(){
 echo "Run businessviews spark job and update hive tables."
 echo "Usage: $0 [source]"
 echo "Examples :"
 echo "       $0 dun"
 echo "       $0 rct"
}

#load config files from hdfs#
for ELEMENT in $(hdfs dfs -ls ${pom.enrichedata.hdfs.conf_schemas_dir}/*.conf | awk  '{print $NF}')
do
 if [ -n "$SCHEMAS_FILES" ]; then
	SCHEMAS_FILES="${SCHEMAS_FILES},${ELEMENT}"
 else
    SCHEMAS_FILES="${ELEMENT}"
 fi
done

for ELEMENT in $(hdfs dfs -ls ${pom.businessviews.hdfs.conf_dir}/*/*/*.avsc | awk  '{print $NF}')
do
 if [ -n "$SCHEMAS_FILES" ]; then
	SCHEMAS_FILES="${SCHEMAS_FILES},${ELEMENT}"
 else
    SCHEMAS_FILES="${ELEMENT}"
 fi
done

DATANUCLEUS_JARS=$(echo /usr/hdp/current/spark-client/lib/datanucleus-*.jar | tr ' ' ',')


spark-submit \
    --master yarn \
    --deploy-mode cluster \
    --queue ${pom.businessviews.yarnqueue} \
    --num-executors 4 --executor-cores 3 --executor-memory 9G \
    --name "Businessviews for ${DATESETNAME}"  \
    --jars "${SCHEMAS_FILES},${DATANUCLEUS_JARS}" \
    --class com.socgen.bsc.cbs.businessviews.Main ${pom.businessviews.hdfs.lib_dir}/${pom.businessviews.spark_jar_name} \
    $@

case "$1" in
  "rct") ${currentDir}/hive_manager.sh --update "rct" ;break ;;
  "avox") ${currentDir}/hive_manager.sh --update "avox" ;break;;
  "lei") ${currentDir}/hive_manager.sh --update "lei" ;break;;
  "insee") ${currentDir}/hive_manager.sh --update "insee" ;break;;
  "grc_pp") ${currentDir}/hive_manager.sh --update "grc" ;break;;
  "grc_pm") ${currentDir}/hive_manager.sh --update "grc" ;break;;
  "grcdelta") ${currentDir}/hive_manager.sh --update "grc" ;break;;
  "grcdeltapp") ${currentDir}/hive_manager.sh --update "grc" ;break;;
  "dun") ${currentDir}/hive_manager.sh --update "dun" ;break;;
  "perle_legalentity") ${currentDir}/hive_manager.sh --update "perle" ;break;;
  "infogreffe") ${currentDir}/hive_manager.sh --update "infogreffe" ;break;;
  "idq") ${currentDir}/hive_manager.sh --update "idq" ;break;;
  "bdr") ${currentDir}/hive_manager.sh --update "bdr" ;break;;
  "privalia") ${currentDir}/hive_manager.sh --update "privalia" ;break;;
  "czsk") ${currentDir}/hive_manager.sh --update "cz" ;${currentDir}/hive_manager.sh --update "sk" ;break;;
  * )  break ;;
esac

