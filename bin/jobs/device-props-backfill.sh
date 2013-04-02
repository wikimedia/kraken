#!/bin/bash

fail () {
    echo; echo "job '$2' failed! ($1)" >&2
    exit $1
}

year=2013
month=03
job_queue=adhoc

for day in `seq -w 1 31`; do
    job_name="device_props_backfill-$year-$month-$day"
    job_input="$nameNode/wmf/data/webrequest/mobile/device/props/$year/$month/$day/**"
    job_output_dir="$nameNode/wmf/public/webrequest/mobile/device/props/$year/$month/$day"
    job_output_file="mobile_device_props-$year-$month-$day.tsv"
    job_output="${job_output_dir}/${job_output_file}"
    
    echo "starting $job_name..."
    `dirname $0`/coalesce -i "$job_input" -o "$job_output" -j "$job_name" -q "$job_queue" -u stats \
        || fail $? "$job_name"
    
    echo; echo
done
