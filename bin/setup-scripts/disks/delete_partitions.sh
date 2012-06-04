#!/bin/bash

# Deletes all partitions created by other scripts in this directory
#
# Deletes partitions sd{e,f,g,h,i,j}{1,2}

# umount all of our experimental partitions.
echo $(mount | egrep '^/dev/(md2|sd[e,f,g,h,i,j])' | awk '{print $3}') | xargs sudo umount -v


# if md2 exists, remove the raid on it
if [ -e /dev/md2 ]; then
    sudo mdadm --manage --stop /dev/md2
fi

for disk in /dev/sd{e,f,g,h,i,j}; do 
    echo "Deleting partitions on $disk"
    sudo fdisk $disk <<EOF
d
1
d
2
w
EOF

done
