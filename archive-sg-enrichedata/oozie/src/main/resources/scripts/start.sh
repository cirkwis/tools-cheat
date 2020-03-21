#!/bin/sh

bin_dir="${pom.businessviews.fs.root_dir}/bin"

default_time_zone="0100"
default_time_start_date="04:00"
default_time_initial_date="04:00"
default_time_end_date="04:00"

start_date=$(date +"%Y-%m-%dT${default_time_start_date}+${default_time_zone}")
initial_date=$(date +"%Y-%m-%dT${default_time_initial_date}+${default_time_zone}")
end_date=$(date +"%Y-01-01T${default_time_end_date}+${default_time_zone}" -d "+ 1 year")

# -D <property=value>   set/override value for given property
# This variable get all parameters for send it to oozie client
export oozie_custom_properties="-D startDate=${start_date} -D initialDate=${initial_date} -D endDate=${end_date} $@"

${bin_dir}/oozie_manager.sh --start
