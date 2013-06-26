# deduplicates webrequest log lines from $1.  Stores output in $2

source=$1
dest=${2:-deduplicated}

hdfs_ls() {
    hadoop fs -ls $@ | sed -e '1d' | grep -vP '^Found .+ items' | awk '{print $NF}'
}


for f in $(hdfs_ls -d $source); do
    # get the bordering data
    files=$(echo $(hdfs_ls -d $source | grep -C 1 ${f}) | tr ' ' ',')
    echo "-- $(date) Deduplicating $f --"
    echo sudo -u hdfs pig -f /opt/kraken/pig/deduplicate.pig -p input="${files}" -p output="${dest}/deduplicated/$(basename $f)";
    sudo -u hdfs pig -f /opt/kraken/pig/deduplicate.pig -p input="${files}" -p output="$(dirname $f)/deduplicated/$(basename $f)";
    echo "  Done."
    echo ''
done
