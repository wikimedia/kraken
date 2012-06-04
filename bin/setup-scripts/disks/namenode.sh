#!/bin/bash

hadoop_name_directory="/var/lib/hadoop-0.20/name"

# HDFS namenode sde and sdf

# sd{e,f}1 become 100G Linux RAID partitions

for disk in /dev/sd{e,f}; do sudo fdisk $disk <<EOF
n
p
1

+100G
t
fd
w
EOF

done

# create mirrored RAID 1 on sde1 and sdf1 ext3
sudo mdadm --create /dev/md2 --level 1 --raid-devices=2 /dev/sde1 /dev/sdf1
sudo mkfs.ext3 /dev/md2

# mount it at $hadoop_name_directory
sudo mkdir  $hadoop_name_directory && sudo chgrp hadoop $hadoop_name_directory
grep -q '$hadoop_name_directory' /etc/fstab || echo -e "# Hadoop namenode partition\n/dev/md2\t$hadoop_name_directory\text3\tdefaults,noatime\t0\t2" | sudo tee -a /etc/fstab

