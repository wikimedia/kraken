#!/bin/bash

kafka_log_directory="/var/lib/kafka"


# sd{c,d,e,f,h,i,j,k,l}1 become full Linux RAID autodetect partitions

for disk in /dev/sd{c,d,e,f,g,h,i,j,k,l}; do sudo fdisk $disk <<EOF
n
p
1


t
fd
w
EOF
done

# create a raid 10 array
sudo mdadm -v --create /dev/md2 --level=raid10 --raid-devices=10 /dev/sdc1 /dev/sdd1 /dev/sde1 /dev/sdf1 /dev/sdg1 /dev/sdh1 /dev/sdi1 /dev/sdj1 /dev/sdk1 /dev/sdl1
# wait until the array is finished; watch /proc/mdstat
# modify mdadm.conf so the array is assembled on boot
sudo mdadm --examine --scan --config=mdadm.conf >> /etc/mdadm/mdadm.conf

# create an ext3 filesystem
sudo mkfs -t ext3 /dev/md2
sudo tune2fs -m 0 /dev/md2
grep -q $kafka_log_directory /etc/fstab || echo -e "# Kafka Log partition is on /dev/md2\n/dev/md2\t$kafka_log_directory\text3\tdefaults,noatime\t0\t2" | sudo tee -a /etc/fstab    

sudo mkdir -pv $kafka_log_directory
sudo chown -v root:kafka $kafka_log_directory
sudo chmod -v 0775 $kafka_log_directory

sudo mount -v $kafka_log_directory

