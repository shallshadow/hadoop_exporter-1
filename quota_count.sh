date=$(date +%Y%m%d)
dir_list="/user/* "
su - hdfs -c "hadoop fs -count -q ${dir_list}" 1> result 2> quota_log/${date}.err.log
