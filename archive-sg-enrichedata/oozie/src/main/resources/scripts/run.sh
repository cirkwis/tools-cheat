#!/usr/bin/env bash

# get Kerberos ticket
if [ `command -v kinit` ]; then
    kinit -kt "${kerberosUser}.applicatif.keytab" "${kerberosPrincipal}"
fi

DATANUCLEUS_JARS=$(echo /usr/hdp/current/spark-client/lib/datanucleus-*.jar | tr ' ' ',')


${pathToSparkSubmit}spark-submit \
                    --master ${master} \
                    --deploy-mode ${deploy} \
                    --num-executors 8 \
                    --executor-memory 15G \
                    --queue ${queue} \
                    --jars ${DATANUCLEUS_JARS} \
                    --driver-class-path . \
                    --class com.socgen.bsc.cbs.businessviews.Main ${pom.businessviews.spark_jar_name} $@
