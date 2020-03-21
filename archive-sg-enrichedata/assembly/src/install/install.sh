#!/bin/sh
#set -x
##############################Configuration###################################
hdfsuser=${pom.cbs.appuser}

cbsHdfsProjectRootDir=${pom.cbs.hdfs.projectroot}
cbsHdfsOutputDataDir=${pom.businessviews.base_output_dir}
appHdfsHome=${pom.businessviews.hdfs.root_dir}
appLocalHome=${pom.businessviews.fs.root_dir}
appHdfsBinDir=${pom.businessviews.hdfs.bin_dir}
appHdfsSparkDir=${pom.businessviews.hdfs.spark_dir}
appHdfsHiveDir=${pom.businessviews.hdfs.hive_dir}
appHiveDataBaseName=${pom.cbs.hive.dbname}

oozie_start_date=$(date +'%Y-%m-%dT00:00+0200')
oozie_end_date=$(date +'%Y-12-31T00:00+0200')

kerberosPrincipal=${pom.cbs.kerberosprincipal}
kerberosKeytab=${pom.cbs.fs.kerberosappkeytabpath}

logDate=$(date +"%Y%m%d_%H%M%S")

current_date=$(date '+%Y%m%d')
current_hour=$(date '+%H:%M:%S')

error=0

lastCmd=""

currentDir=$(dirname "$(readlink -f "$0")")

script_name=`echo "$(basename $0)" | cut -d'.' -f1`

tmpDir="${pom.cbs.fs.projectroot}/archives/${current_date}/businessviews_${current_hour}"

backupDir=$tmpDir

hiveDir="${currentDir}/hive"

logFile="${tmpDir}/deploy.log"

##############################################################################
# FUNCTIONS
##############################################################################
# Function to help the user
function usage() {
	echo -e "\r\tThis script install Businessviews CBS Back"

	echo -e "\tOPTIONS:"
	echo -e "\t--help	Help"
	exit 0;
}

function initialize_temp_directory(){
  mkdir -p ${tmpDir}
}
# Function to initialize kerberos ticket
function initialize_kerberos_ticket() {
	# Int and Prd
	if [[ ${env} == 'hom' ]] || [[ ${env} == 'prd' ]]; then
		log "Logging to Kerberos as ${kerberosPrincipal} using ${kerberosKeytab}"
		execute "kinit -k ${kerberosPrincipal} -t ${kerberosKeytab}"
	fi
}

# Function to log messages
function log() {
	echo -e "$(date +"%Y/%m/%d %H:%M:%S") [INFO] $1" | tee -a ${logFile}
	lastCmd="$1"
}

# Function to check last action success
function check() {
	if (( $? != 0 )); then
		echo -e "$(date +"%Y/%m/%d %H:%M:%S") [ERROR] $lastCmd [KO]" | tee -a ${logFile}
		let error=error+1
	fi
}

# Function to execute  actions and log them
function execute () {
    lastCmd="$1"
	echo -e "$(date +"%Y/%m/%d %H:%M:%S") [EXEC] $1" | tee -a ${logFile}
	$1 1>> ${logFile} 2>&1
	check
}

# Function to welcome the user
function hello() {
	log  "You are running the script $0."
}

# Function to trap exit signal
function end(){
	log "${error} errors have been detected during the execution."
	log "For more information, please consult: ${logFile}"
}

function backup_HDFS_dir_to_FS(){
 srcHdfsDir=$1
 destFsDir=$2
 title=$3

 hdfs dfs -test -d ${srcHdfsDir}
 if [ $? -eq 0 ]
 then
  log "Recuperation de la version de ${title} sur le hdfs."
  execute "hdfs dfs -copyToLocal  ${srcHdfsDir} ${destFsDir}"
 else
  log "Pas de dossier ${title} sur le hdfs!"
 fi
}

function backup_FS_dir_to_FS(){
 srcFsDir=$1
 destFsDir=$2
 title=$3

 if [ -d ${srcFsDir} ]
 then
  log "Recuperation de la version de ${title} sur le local fs."
  execute "cp -r ${srcFsDir} ${destFsDir}"
 else
  log "Pas de dossier d'installation ${title} sur local fs!"
 fi
}

function backup(){
 log "****** begin backup_cbs() ******"
 execute "mkdir -p ${tmpDir}/${appDirName}/HDFS"
 execute "mkdir -p ${tmpDir}/${appDirName}/EDGE_FS"

 backup_HDFS_dir_to_FS "${appHdfsHome}" "${tmpDir}/${appDirName}/HDFS" "Businessviews"
 backup_FS_dir_to_FS "${appLocalHome}" "${tmpDir}/${appDirName}/EDGE_FS" "Businessviews"

 log "Archivage du backup."
 cd ${tmpDir}
 execute "zip -r ${tmpDir}/${appDirName}_bckp_${logDate}.zip ${appDirName}"

 log "New backup is here : ${tmpDir}/${appDirName}_bckp_${logDate}.zip"
 log "****** end backup_cbs() ******"
}

function clean_HDFS_dir(){
 srcHdfsDir=$1
 title=$2

 hdfs dfs -test -d ${srcHdfsDir}
 if [ $? -eq 0 ]
 then
  log "Delete content of hdfs ${title} directory."
  execute "hdfs dfs -rm -R ${srcHdfsDir}"
 fi
 log "Create new hdfs ${title} directory."
 execute "hdfs dfs -mkdir -p ${srcHdfsDir}"
}

function clean_FS_dir(){
 srcFsDir=$1
 title=$2

 if [ -d ${srcFsDir} ]
 then
  log "Delete content of local fs ${title} directory"
  execute "rm -rf ${srcFsDir}"
 fi
 log "Create new local fs ${title} directory"
 execute "mkdir -p ${srcFsDir}"

}

function businessviews_clean_HDFS(){
  clean_HDFS_dir "$appHdfsHome" "Businessviews"
}

function businessviews_clean_FS(){
  clean_FS_dir "${appLocalHome}" "Businessviews"
}

function initialize_directorys_witout_data(){
  log "****** begin Businessviews() ******"
  businessviews_clean_HDFS
  businessviews_clean_FS

  hdfs dfs -test -d "${appHdfsHome}"
  if [ $? -ne 0 ]
  then
   log "Creation of HDFS data directory because they dont exist."
   execute "hdfs dfs -mkdir -p ${cbsHdfsProjectRootDir}/data"
  fi

  log "****** end Businessviews() ******"
}

function install_in_hdfs(){
  log "****** begin install_in_hdfs ******"

  execute "hdfs dfs -put  ${currentDir}/HDFS/* ${appHdfsHome}"

  execute "hdfs dfs -chmod +x  ${appHdfsBinDir}/*.sh"
  execute "hdfs dfs -chmod +x  ${appHdfsSparkDir}/*.sh"

  log "****** end install_in_hdfs ******"
}

function install_in_fs(){
 log "****** begin install_in_fs ******"

 execute "cp -fr ${currentDir}/EDGE_FS/* ${appLocalHome}"
 execute "chmod +x  ${appLocalHome}/bin/*.sh"

 log "****** end install_in_fs ******"
}
function hive_exe_queries_from_hdfs_files(){
 file_wildcard=$1
 tmpscriptfile="${currentDir}/~hive_tmp_"$logDate
 #init tmp file
 echo "" > ${tmpscriptfile}

 #load hive sql file from hdfs by mask #
 for ELEMENT in $(hdfs dfs -ls ${appHdfsHiveDir}/*${file_wildcard}*.sql | awk  '{print $NF}')
 do
  hdfs dfs -cat "${ELEMENT}" >> "${tmpscriptfile}"
 done
 execute "hive --hivevar cbsBase=${appHiveDataBaseName} --hivevar hdfsDataOutputLocation=${cbsHdfsOutputDataDir} -f ${tmpscriptfile}"

 execute "rm -f ${tmpscriptfile}"
}
function install_hive(){
 log "****** begin install_hive ******"
 log "hive -e 'CREATE DATABASE IF NOT EXISTS ${appHiveDataBaseName};'"
 hive -e "CREATE DATABASE IF NOT EXISTS ${appHiveDataBaseName};"
 hive_exe_queries_from_hdfs_files "create"
 hive_exe_queries_from_hdfs_files "update"
 log "****** end install_hive ******"
}

###############################################################################
## INITIALIZATION
###############################################################################

initialize_temp_directory

initialize_kerberos_ticket

trap end EXIT

###############################################################################
## INSTALL
###############################################################################
## Welcome message
hello
appDirName=$(basename $appHdfsHome)

backup
initialize_directorys_witout_data
install_in_hdfs
install_in_fs
#dirty ack for escape hive table installation
if [ "$1" == "-h" ] ;then
 install_hive;
fi

exit 0
