# deduplicates webrequest log lines from $1.  In dirname $1/deduplicated
source=$1
pig_script="$(dirname $(dirname $0))/pig/deduplicate.pig"

hdfs_ls() {
    hadoop fs -ls $@ | sed -e '1d' | grep -vP '^Found .+ items' | awk '{print $NF}'
}

get_upper_time_bound() {
    lower_time_bound=$1
    frequency='15'  # minutes
    python -c "
from datetime import datetime, timedelta
lower = datetime.strptime('$lower_time_bound', '%Y-%m-%d_%H.%M.%S')
delta = timedelta(minutes=$frequency)
upper = lower + delta
print(upper.strftime('%Y-%m-%d_%H.%M.%S'))
"
}


for f in $(hdfs_ls -d $source); do
    # get the lower and upper time bounds
    base_name=$(basename ${f})
    lower_time_bound=${base_name:3} # strip off the dt= from the dirname
    upper_time_bound=$(get_upper_time_bound $lower_time_bound)
    
    echo "-- $(date) Deduplicating $f ($lower_time_bound - $upper_time_bound) --"
    # get the bordering data
    files=$(echo $(hdfs_ls -d $source | grep -C 1 ${f}) | tr ' ' ',')
    echo "   " sudo -u hdfs pig -f $pig_script -p lower_time_bound=${lower_time_bound} -p upper_time_bound=${upper_time_bound} -p input="${files}" -p output="$(dirname $f)/deduplicated/$(basename $f)";
    sudo -u hdfs pig -f $pig_script -p lower_time_bound=${lower_time_bound} -p upper_time_bound=${upper_time_bound} -p input="${files}" -p output="$(dirname $f)/deduplicated/$(basename $f)";
    echo "   Done."
    echo ''
done


