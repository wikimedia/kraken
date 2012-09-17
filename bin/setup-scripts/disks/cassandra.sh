#!/bin/bash
#
# sd{e,f,g,h,i,j}2 become 100G partitions

cassandra_commitlog_directory="/var/lib/cassandra/commitlog"
cassandra_data_directory="/var/lib/cassandra/data"

# If we are running RAID on any of these disks,
# we need to stop it before we can write out
# changes to the partition tables.  (This should
# only be true on analytics1001 namenode.)

if [ -e /dev/md2 ]; then
    sudo mdadm --stop /dev/md2
fi
if [ -e /dev/md_d2 ]; then
    sudo mdadm --stop /dev/md_d2
fi

for disk in /dev/sd{e,f,g,h,i,j}; do sudo fdisk $disk <<EOF
n
p
2

+100G
w
EOF

done

# For some reason, mdadm restarts our RAID array
# as /dev/md_d2 after writing the changes out to 
# sde and sdf.  If this device exists, stop the
# md_d2 device and create the md2 one.
if [ -e /dev/md_d2 ]; then
    sudo mdadm --stop /dev/md_d2
    sudo mdadm --create /dev/md2 --level 1 --raid-devices=2 /dev/sde1 /dev/sdf1 <<EOF
y
EOF
fi

# sde2 is the commitlog_directory.
# ext3 and mounted at /var/lib/cassandra/commitlog
sudo -u cassandra mkdir $cassandra_commitlog_directory
sudo mkfs.ext3 /dev/sde2
grep -q "$cassandra_commitlog_directory" /etc/fstab || echo -e "# Cassandra commitlog partition\n/dev/sde2\t$cassandra_commitlog_directory\text3\tdefaults,noatime\t0\t2" | sudo tee -a /etc/fstab
sudo mount $cassandra_commitlog_directory

# sd{f,g,h,i,j}2 are data_file_directories.
# xfs and mounted at /var/lib/cassandra/data/data_X
sudo -u cassandra mkdir -p $cassandra_data_directory
for disk_letter in f g h i j ; do 
    partition="/dev/sd${disk_letter}2"
    data_directory="$cassandra_data_directory/${disk_letter}"
    
    sudo -u cassandra mkdir -p $data_directory
    
    sudo mkfs.xfs $partition
    grep -q $data_directory /etc/fstab || echo -e "# Cassandra data partition $disk_letter\n$partition\t$data_directory\txfs\tdefaults,noatime\t0\t2" | sudo tee -a /etc/fstab
done
    