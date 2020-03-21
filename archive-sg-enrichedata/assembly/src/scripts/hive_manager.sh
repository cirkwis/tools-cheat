#!/bin/sh

appHdfsHiveDir="${pom.businessviews.hdfs.hive_dir}"
appHiveDataBaseName="${pom.cbs.hive.dbname}"
cbsHdfsOutputDataDir="${pom.businessviews.base_output_dir}"
cbsQueueYarn="${pom.businessviews.yarnqueue}"


function usage(){
 echo "Execute hive script file from hdfs.${appHdfsHiveDir}"
 echo "Source parameter is used to found script in hdfs with this mask : *source_[delete|create|update]*.sql"
 echo "Usage: "
 echo "   $0 [-d|--delete, -c|--create,  -u|--update] source"
 echo "   $0 -s|--show #list all hive script present in cluster."
 echo "   $0 -h|--help"
 echo "Examples :"
 echo "       $0 -d grc"
 echo "       $0 --create grc"
 echo "       $0 -u grcdelta"
 echo "       $0 --delete dun"
 echo "       $0 -s"
}

function hive_exe_script_from_hdfs_files(){
 file_wildcard=$1
 #load hive sql file from hdfs by mask #
 for ELEMENT in $(hdfs dfs -ls ${appHdfsHiveDir}/*${file_wildcard}*.sql | awk  '{print $NF}')
 do
  hive  --hiveconf tez.queue.name=${cbsQueueYarn} --hivevar cbsBase=${appHiveDataBaseName} --hivevar hdfsDataOutputLocation=${cbsHdfsOutputDataDir} -f ${ELEMENT}
 done
}
function hive_delete(){
 source=$1
 if [ -n "$source" ]; then
	hive_exe_script_from_hdfs_files "${source}_delete"
 fi
}
function hive_create(){
 source=$1
 if [ -n "$source" ]; then
	hive_exe_script_from_hdfs_files "${source}_create"
 fi
}
function hive_update(){
 source=$1
 if [ -n "$source" ]; then
	hive_exe_script_from_hdfs_files "${source}_update"
 fi
}
function hive_show_all_script_files(){
 hdfs dfs -ls ${appHdfsHiveDir}/*.sql | awk  '{print $NF}'
}

if [[ "$#" -ne "1" ]] && [[ "$#" -ne "2" ]]; then
 echo "Invalid nb arguments"; usage >&2 ; exit 1 ;
fi

OPTS=`getopt -o d:c:u:sh --long delete:,create:,update:,show,help -- "$@"`
if [ $? != 0 ] ; then echo "Failed parsing parameter."; usage >&2 ; exit 1 ; fi

eval set -- "$OPTS"

while true; do
 case "$1" in
   -d | --delete ) hive_delete "$2"; break ;;
   -c | --create ) hive_create "$2"; break ;;
   -u | --update ) hive_update "$2"; break ;;
   -s | --show ) hive_show_all_script_files; break ;;
   -h | --help )  usage; break ;;
   -- ) shift; break ;;
   * )  break ;;
 esac
done




