#!/bin/bash

generate_file() {

	local input_file=$1
	local limit=$2
	local results

	output_file=/tmp/results.txt

	readarray results < $input_file

	rm $output_file
	for ((i=0; i<$limit; i++)); do echo ${results[i]} >> $output_file
	done
}

experiment(){
	source executor/setup.sh

	local input_file=$1
	local ips=$2
	local project_folder=$3

	local output_file=results.dat
	local output_string
	local output_speed_up

	rm $output_file
	touch $output_file

	local sequential

	for i in `seq 1 1 10`; do
		generate_file $ips $i
		echo "[EXPERIMENT] ${i} Computers"
		local result=$(map_reduce $input_file $project_folder /tmp/results.txt | grep "PROGRAM-TOTAL" | awk '{print $2}')
		if [[ $i == 1 ]];
		then
			sequential=$result
		fi

		output_string+="${i}\t${result}\n" 

		speed_up=$(calc $sequential/$result)

		speed_up=$(echo $speed_up | sed 's/\~//g')
		output_speed_up+="${i}\t${speed_up}\n"
	done

	rm results.dat speed_up.dat

	printf $output_string | tee -a results.dat
	printf $output_speed_up | tee -a speed_up.dat

	gnuplot -e "input_file='results.dat'; output_file='latency.eps'" experiments/plotter.plg
	gnuplot -e "input_file='speed_up.dat'; output_file='speed_up.eps'" experiments/speed_up_plotter.plg
}
