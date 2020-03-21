#!/bin/sh
currentDir=$(dirname "$(readlink -f "$0")")
local_deploy_logs_dir="${currentDir}/assembly/target/deploy_logs"

DEV_SERVER="dhadlx54.dns21.socgen"
SSH_USER=""
HIVE_DB_INSTALL=""
HDFS_APP_USER="haddadm"
TMP_INSTALL_DIR="businessviews_deploy_tmp_$(date +"%Y%m%d_%H%M%S")"

function usage(){
 echo ""
 echo "Deploy the current build package to DEV environement with ssh."
 echo "And put de deploy log file here [${local_deploy_logs_dir}]."
 echo "Requirement : "
 echo " - Generate your ssh rsa key pair on you host and dev server system : "
 echo "       ssh-keygen -t rsa"
 echo " - Copy your host public key to ~/.ssh/authorized_keys file on dev server :"
 echo '       cat ~/.ssh/id-rsa.pub | ssh user@dhadlx54.dns21.socgen "cat && >> ~/.ssh/authorized_keys"'
 echo ""
 echo " - For compatibility algorithms between ssh client/server you may add this configuration on you host ssh config file(~/.ssh/config)[https://www.openssh.com/legacy.html]."
 echo "       Host dhadlx54.dns21.socgen"
 echo "           HostkeyAlgorithms +ssh-dss"
 echo ""
 echo "Usage:"
 echo "   $0 -u|--user [USER_ACCOUNT_IN_DEV_SERVER] -h|--hive"
 echo ""
 echo "-u|--user [USER_ACCOUNT_IN_DEV_SERVER]:"
 echo "     Mandatory parameter to user the good ssh account user to deploy."
 echo ""
 echo "-d|--hivedb :"
 echo "     Parameter to create hive database and initialize hive tables."
 echo ""
 echo "-h|--help :"
 echo "     Print usage."
 echo ""
 echo "Examples : "
 echo "     $0 --user x150102 "
 echo ""
 echo "     $0 --user x150102 --hivedb"
 echo ""
}

function check_ssh_user_connection(){
 ssh -q -o PasswordAuthentication=no "${SSH_USER}@${DEV_SERVER}" exit
 if (( $? != 0 )); then
 	echo "[ERROR] SSH connection fail! Please check your credential!"
 	usage
 	exit 1
 fi
}

function check(){
	if (( $? != 0 )); then
		echo "[ERROR] Last command fail! Stoping installation!"
		exit 1
	fi
}

function get_package(){
 BUILD_PACKAGE_PATH=$(find  "${currentDir}/assembly/target" -maxdepth 1 -type f -name cbs_businessviews*.zip)
 check
 if [ ! -f "${BUILD_PACKAGE_PATH}" ];then
  echo "[ERROR] - No package found in directory ${currentDir}/assembly/target. Please build project with maven!"
  exit 1
 fi
 echo "Get package file : ${BUILD_PACKAGE_PATH}."
}

function deploy_package(){
 echo "Create tmp directory in Dev (${DEV_SERVER}:/tmp/${TMP_INSTALL_DIR})."
 ssh -q -o PasswordAuthentication=no "${SSH_USER}@${DEV_SERVER}" <<EOF
  mkdir "/tmp/${TMP_INSTALL_DIR}"
  chmod -R 777  "/tmp/${TMP_INSTALL_DIR}"
EOF
 check
 echo "Deploying package ($(basename ${BUILD_PACKAGE_PATH})) to DEV server."
 scp -q ${BUILD_PACKAGE_PATH}  "${SSH_USER}@${DEV_SERVER}:/tmp/${TMP_INSTALL_DIR}"
 check
}

function run_install_package(){
 echo "Install begin."
 ssh -q -o PasswordAuthentication=no "${SSH_USER}@${DEV_SERVER}" <<EOF
  sudo su - ${HDFS_APP_USER}
  cd /tmp/${TMP_INSTALL_DIR}
  unzip $(basename ${BUILD_PACKAGE_PATH})
  ./install.sh ${HIVE_DB_INSTALL}
EOF
 check
}

function collect_install_package_logs(){
 echo "Get installation log file in [${local_deploy_logs_dir}]."
 if [ ! -d "${local_deploy_logs_dir}" ]; then  mkdir -p ${local_deploy_logs_dir}; fi
 scp -q "${SSH_USER}@${DEV_SERVER}:/tmp/${TMP_INSTALL_DIR}/tmp_deploy_*/deploy.log" ${local_deploy_logs_dir}
 check
}

OPTS=`getopt -o u:dh --long user:,hivedb,help -- "$@"`
if [ $? != 0 ] ; then echo "Failed parsing parameter."; usage >&2 ; exit 1 ; fi
eval set -- "$OPTS"

while true; do
 case "$1" in
   -u  | --user )    SSH_USER="$2"; shift 2 ;;
   -d  | --hivedb )  HIVE_DB_INSTALL="-h"; shift ;;
   -h  | --help )    usage; exit 0 ;;
   -- )              shift; break;;
   * )               break ;;
 esac
done

echo "*************************************************************************"
echo "*                    Install to Dev Cluster BEGIN                      *"
echo "*************************************************************************"
check_ssh_user_connection
get_package
deploy_package
run_install_package
collect_install_package_logs
echo "*************************************************************************"
echo "*                             Install END                               *"
echo "*************************************************************************"
echo ""










 