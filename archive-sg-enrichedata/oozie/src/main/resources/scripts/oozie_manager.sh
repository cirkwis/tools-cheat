#!/bin/sh
source  "${pom.businessviews.fs.root_dir}/bin/oozie_lib_tools.sh"
oozie_hdfs_home_dir="${pom.businessviews.hdfs.oozie_dir}"
logDate=$(date +"%Y%m%d_%H%M%S")
currentDir=$(dirname "$(readlink -f "$0")")
tmp_job_properties_file="${currentDir}/~tmp_${logDate}_job.properties"

function usage(){
 echo "Manage Oozie for Businessviews application."
 echo "Usage: "
 echo "   #Start Businessviews bundle. Without custom date parameter,"
 echo "   #it start bundle from the current date to the end of the year."
 echo "   $0 [-s|--start] "
 echo "   $0 [-e|--stop]    #Try to stop gently businessviews bundle."
 echo "   $0 [-l|--list] [b|bundle, c|coordinator, w|workflow]"
 echo "   $0 [-k|--kill] jobId #Kill bundle,coordinator or workflow."
 echo "   $0 -h|--help"
 echo "Examples :"
 echo "       $0 -s"
 echo "       $0 --start"
 echo "       $0 --stop"
 echo "       $0 -e"
 echo "       $0 -l coordinator"
 echo "       $0 --list coordinator"
 echo "       $0 --list w"
 echo "       $0 -k jobId"
}

function start(){
 echo "oozie_custom_properties = ${oozie_custom_properties}"

 if [ "$(exist_active_bundle)" -gt 0  ]
 then
  echo "ERROR : There is active similar bundle(s)! Please deactivate them before go further."
  echo "$(list_active_bundle)"
  exit 1
 fi

 #Get job.properties from hdfs to edge on tmp file.
 hdfs dfs -cat "${oozie_hdfs_home_dir}/job.properties" > "${tmp_job_properties_file}"

 #Try to start the bundle
 response=$(oozie job ${oozie_url_opt_cli} \
                -config "${tmp_job_properties_file}"  ${oozie_custom_properties} -submit 2>&1)
 exe_result=$?

 #clean tmp file
 rm -f "${tmp_job_properties_file}"

 #check error
 if (( $exe_result != 0 )); then
 	echo -e "ERROR : Bundle don't start!\n${response}"
 	exit 1
 else
    BundleId=$(echo "${response}" | awk  '{print $NF}')
    echo "Bundle ${bundle_name} started with JOB_ID : ${BundleId}"
    exit 0
 fi
}

function job_kill(){
 job_id=$1
 oozie job ${oozie_url_opt_cli} -kill "${job_id}"
}

function stop(){
 if [ "$(exist_active_wkf)" -gt 0  ]
 then
  echo "ERROR : There is active workflows on oozie! Please deactivate them before go further."
  echo "$(list_active_wkf)"
  exit 1
 fi

 if [ "$(get_nb_active_bundle)" -gt 0  ]
 then
  for BUNDLE in "$(list_active_bundle | awk  '{print $1}')"
  do
   echo "Killing bundle ${bundle_name} with JOB_ID : ${BUNDLE}."
   job_kill "${BUNDLE}"
  done
 else
   echo "There is not bundle to stop."
 fi
 exit 0
}

function list(){
 type=$1

 case "${type}" in
   "b" | "bundle" ) list_active_bundle; break ;;
   "c" | "coordinator" ) list_active_coordinator; break ;;
   "w" | "workflow"  ) list_active_wkf; break ;;
   * )  echo "Wrong filter : $0 [-l|--list] [b|bundle, c|coordinator, w|workflow]"; break ;;
 esac
}

#----------------------------- Main ------------------------------------------#

if [ $# == 0 ] ; then echo "Argument missing!"; usage >&2 ; exit 1 ; fi

OPTS=`getopt -o sel:k:h --long start,stop,list:,kill:,help -- "$@"`
if [ $? != 0 ] ; then echo "Failed parsing parameter."; usage >&2 ; exit 1 ; fi

eval set -- "$OPTS"

while true; do
 case "$1" in
   -s | --start ) start; break ;;
   -e | --stop )  stop; break ;;
   -l | --list )  list $2; break ;;
   -k | --kill )  job_kill $2; break ;;
   -h | --help )  usage; break ;;
   -- ) shift; break ;;
   * )  break ;;
 esac
done

