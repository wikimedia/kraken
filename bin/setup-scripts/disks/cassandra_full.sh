#!/bin/bash
#
# sd{e,f,g,h,i,j}1 become physical partitions that span the full disks

cassandra_commitlog_directory="/var/lib/cassandra/commitlog"
cassandra_data_directory="/var/lib/cassandra/data"

for disk in /dev/sd{e,f,g,h,i,j}; do sudo fdisk $disk <<EOF
n
p
1


w
EOF

done

# sde1 is the commitlog_directory.
# ext3 and mounted at /var/lib/cassandra/commitlog
sudo -u cassandra mkdir $cassandra_commitlog_directory
sudo mkfs.ext3 /dev/sde1
grep -q "$cassandra_commitlog_directory" /etc/fstab || echo -e "# Cassandra commitlog partition\n/dev/sde1\t$cassandra_commitlog_directory\text3\tdefaults,noatime\t0\t2" | sudo tee -a /etc/fstab
sudo mount $cassandra_commitlog_directory

# sd{f,g,h,i,j}1 are data_file_directories.
# xfs and mounted at /var/lib/cassandra/data/data_X
sudo -u cassandra mkdir -p $cassandra_data_directory
for disk_letter in f g h i j ; do 
    partition="/dev/sd${disk_letter}1"
    data_directory="$cassandra_data_directory/${disk_letter}"
    
    sudo -u cassandra mkdir -p $data_directory
    
    sudo mkfs.xfs -f $partition
    grep -q $data_directory /etc/fstab || echo -e "# Cassandra data partition $disk_letter\n$partition\t$data_directory\txfs\tdefaults,noatime\t0\t2" | sudo tee -a /etc/fstab
done


