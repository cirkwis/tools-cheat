#!/bin/sh
# Oozie documentation used for this code.
# https://oozie.apache.org/docs/3.1.3-incubating/DG_CommandLineTool.html#Starting_a_Workflow_Coordinator_or_Bundle_Job

MAX_RESULT=500

bundle_filter_status_active="status=PREP;status=RUNNING;status=RUNNINGWITHERROR;status=PREPPAUSED"
coordinator_filter_status_active="status=PREP;status=PREPPAUSED;status=PREPSUSPENDED;status=RUNNING;status=RUNNINGWITHERROR;status=SUSPENDED;status=SUSPENDEDWITHERROR"
wkf_filter_status_active="status=RUNNING;status=SUSPENDED"

bundle_name='[CBS-Bundle-BusinessViews-${pom.businessviews.appenv}]'
bundle_name_escape=' \[CBS\-Bundle\-BusinessViews\-${pom.businessviews.appenv}\]'
coordinator_name=' \[CBS\-Coord\-BusinessViews\-[a-zA-Z0-9]*\-${pom.businessviews.appenv}\]'
wkf_name=' \[CBS\-Wkf\-BusinessViews\-[a-zA-Z0-9]*\-${pom.businessviews.appenv}\]'

CBS_DEFAULT_OOZIE_URL="${pom.cbs.oozie.url}"
oozie_url_opt_cli=""

function init_oozie_url_opt_cli(){
 if [ "${OOZIE_URL}" == "" ]; then
  echo "Env OOZIE_URL is not set. So we use CBS_DEFAULT_OOZIE_URL -> ${CBS_DEFAULT_OOZIE_URL}"
  if [ "${CBS_DEFAULT_OOZIE_URL}" == "" ]; then
   oozie_url_opt_cli=""
  else
   oozie_url_opt_cli=" -oozie ${CBS_DEFAULT_OOZIE_URL} "
  fi
 else
  echo "Env OOZIE_URL is set to system. So we use it OOZIE_URL -> ${OOZIE_URL}"
  oozie_url_opt_cli=" -oozie ${OOZIE_URL} "
 fi
}

function list_active_bundle(){
 echo "$(oozie jobs ${oozie_url_opt_cli} -jobtype bundle \
                   -filter "${bundle_filter_status_active};name=${bundle_name}" | grep "${bundle_name_escape}")"
}

function get_nb_active_bundle(){
 echo "$(oozie jobs ${oozie_url_opt_cli} -jobtype bundle \
                   -filter "${bundle_filter_status_active};name=${bundle_name}" \
                   | grep "${bundle_name_escape}" \
                   | wc -l)"
}

function exist_active_bundle(){
 if [ "$(get_nb_active_bundle)" -gt 0 ]
 then
  echo "1"
 else
  echo "0"
 fi
}
##############################################################

function list_active_coordinator(){
 echo "$(oozie jobs ${oozie_url_opt_cli} -len $MAX_RESULT -jobtype coordinator \
                   -filter "${coordinator_filter_status_active}" | grep "${coordinator_name}")"
}

function get_nb_active_coordinator(){
 echo "$(oozie jobs ${oozie_url_opt_cli} -len $MAX_RESULT -jobtype coordinator \
                   -filter "${coordinator_filter_status_active}" | grep "${coordinator_name}" | wc -l)"
}

function exist_active_coordinator(){
 if [ "$(get_nb_active_coordinator)" -gt 0  ]
 then
  echo "1"
 else
  echo "0"
 fi
}
#############################################################
function list_active_wkf(){
 echo "$(oozie jobs ${oozie_url_opt_cli} -len $MAX_RESULT \
                   -filter "${wkf_filter_status_active}" | grep "${wkf_name}")"
}

function get_nb_active_wkf(){
 echo "$(oozie jobs ${oozie_url_opt_cli} -len $MAX_RESULT \
                   -filter "${wkf_filter_status_active}" | grep "${wkf_name}" | wc -l)"
}

function exist_active_wkf(){
 if [ "$(get_nb_active_wkf)" -gt 0  ]
 then
  echo "1"
 else
  echo "0"
 fi
}

init_oozie_url_opt_cli
