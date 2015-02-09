#!/bin/bash
# ./aws_partition_redirect.sh myKey.pem 52.0.104.140 52.0.104.141 52.0.104.142

if [ "$#" -lt "2" ]; then
    echo "Illegal number of parameters"
    echo "Example : ./aws_partition_redirect.sh myKey.pem 52.0.104.140 52.0.104.141 52.0.104.142"
    echo "Note : first host is the master"
    exit 1
fi


# get the pem file
pem=$1
shift
# get the master host
master=$1

# launch initialize master script
ssh -i $pem -o StrictHostKeyChecking=no ubuntu@$master 'bash -s' < 'initialize_master.sh'

# initialize forwarding port for spark ui
for (( p=0; p<=$#; p++ ))
do
	echo "ssh -L 808$p:127.0.0.1:808$p -nNT -i $pem -o StrictHostKeyChecking=no ubuntu@$master &"
	ssh -L 808$p:127.0.0.1:808$p -nNT -i $pem -o StrictHostKeyChecking=no ubuntu@$master &
done
echo "ssh -L 4040:127.0.0.1:4040 -nNT -i $pem -o StrictHostKeyChecking=no ubuntu@$master &"
ssh -L 4040:127.0.0.1:4040 -nNT -i $pem -o StrictHostKeyChecking=no ubuntu@$master &


# redirect spark rdd directories for each hosts
for host in "$@"
do
  echo "redirect spark rdd directories for $host"
  ssh -i $pem -o StrictHostKeyChecking=no ubuntu@$host "sudo mkdir /raid0/spark/; sudo mv /var/lib/spark/* /raid0/spark/; sudo rmdir /var/lib/spark/; sudo ln -s /raid0/spark/ /var/lib/"
done



