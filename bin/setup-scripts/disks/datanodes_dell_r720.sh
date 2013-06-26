#!/bin/bash

hadoop_data_directory="/var/lib/hadoop/data"


# HDFS datanodes
#

# for disk in /dev/sd{c,d,e,f,g,h,i,j,k,l}; do sudo fdisk $disk <<EOF
# n
# p
# 1
# 
# 
# w
# EOF
# 
# done


# sdX1 are HDFS data directories
# ext3 and moutned at /var/lib/hadoop/data/X
hadoop_data_directory="/var/lib/hadoop/data"
sudo mkdir -p $hadoop_data_directory
for disk_letter in c d e f g h i j k l; do 
    partition="/dev/sd${disk_letter}1"
    data_directory="$hadoop_data_directory/${disk_letter}"
    
    sudo mkdir -pv $data_directory    
    sudo mkfs.ext3 $partition &
done

hadoop_data_directory="/var/lib/hadoop/data"
for disk_letter in c d e f g h i j k l; do 
    partition="/dev/sd${disk_letter}1"
    data_directory="$hadoop_data_directory/${disk_letter}"
    mkdir -p $data_directory
    grep -q $data_directory /etc/fstab || echo -e "# Hadoop data partition $disk_letter\n$partition\t$data_directory\text3\tdefaults,noatime\t0\t2" | sudo tee -a /etc/fstab    
    sudo tune2fs -m 0 $partition
    
    sudo mount -v $data_directory
done

