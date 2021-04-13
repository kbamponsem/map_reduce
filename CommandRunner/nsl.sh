#!/bin/bash

mapfile -t -c 1 lines < remotemachines.txt
ips=remoteips.txt
for i in "${lines[@]}"
do
	results=$(nslookup $i | grep "Address" | awk '{print $2}')
	echo -e $results | awk '{print $2}' >> $ips
done



