The bash script in this directory are used to setup disk partitions
on Hadoop/HDFS (CDH3) and Hadoop/Cassandra (DSE) for analytics experiments
in the Kraken cluster.  Use these for quick tear down and re-partitioning
of disks while experiments are taking place.  

NOTE.  fdisk will not allow you to write to the partition table while a
disk has a partition mounted.  These scripts do not mount any partitions.
Once you have run them on the appropriate nodes, run 'sudo mount -a' to 
mount them.

# Usage


## namenode.sh

On a namenode (analytics1001), run

  bash namenode.sh
  
This will create /dev/md2, a RAID 1 (mirrored) device acroos /dev/sde1 and 
/dev/sdf1, create and ext3 filesystem, and mount it at $hadoop_name_directory.


## datanodes.sh
On each fresh data node, run

  bash datanodes.sh
  
This will create 6 ext3 partitions each mounted at $hadoop_data_direcotry/data_X,
where 'X' is the letter name of the drive being used for the partition.


## cassandra.sh

On each cassandra node, run

  bash cassandra.sh
  
This will create 1 ext3 partition on /dev/sde2 and mount it at
$cassandra_commitlog_directory.  It will also create 5 xfs partitions
and mount them at $cassandra_data_directory/data_X, where _'X' is the letter
name of the drive being used for the partition.


## delete_partitions.sh

This will unmount and delete all partitions created by each of the other
three scripts.  Note that this will not (currently) remove the /etc/fstab
entries.  Remember to do so manually.


  