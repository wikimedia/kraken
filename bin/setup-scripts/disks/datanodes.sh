#!/bin/bash

hadoop_data_directory="/var/lib/hadoop-0.20/data"


# HDFS datanodes
#
# sd{e,f,g,h,i,j}1 become 100G partitions

for disk in /dev/sd{e,f,g,h,i,j}; do sudo fdisk $disk <<EOF
n
p
1

+100G
w
EOF

done


# sd{e,f,g,h,i,j}1 are HDFS data directories
# ext3 and moutned at /var/lib/hadoop-0.20/data/data_X
sudo mkdir -p $hadoop_data_directory
sudo chgrp hadoop $hadoop_data_directory
for disk_letter in e f g h i j ; do 
    partition="/dev/sd${disk_letter}1"
    data_directory="$hadoop_data_directory/data_${disk_letter}"
    
    sudo mkdir -p $data_directory
    sudo chgrp hadoop $data_directory
    
    sudo mkfs.ext3 $partition
    grep -q $data_directory /etc/fstab || echo -e "# Hadoop data partition $disk_letter\n$partition\t$data_directory\text3\tdefaults,noatime\t0\t2" | sudo tee -a /etc/fstab
done