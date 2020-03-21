#!/usr/bin/env bash

# get Kerberos ticket
if [ `command -v kinit` ]; then
    kinit -kt "${kerberosUser}.applicatif.keytab" "${kerberosPrincipal}"
fi

${pathToSparkSubmit}spark-submit \
                --master ${master} \
                --deploy-mode ${deploy} \
                --num-executors ${pom.businessviews.bt.num-executors} --executor-cores ${pom.businessviews.bt.executor-cores} --executor-memory ${pom.businessviews.bt.executor-memory} \
                --queue ${queue} \
                --driver-class-path . \
                --class com.socgen.bsc.cbs.businessviews.thirdParties.Main ${pom.businessviews.spark_jar_name} $1
