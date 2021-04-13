#!/bin/bash


help() {
	printf "args:\n\t--data-file=[input text file]\n\t--project-path=[your project path]\n\t--ips=[your ips file]\n"
}

if [ $# == 0 ];
then
	printf "Please provide 3 arguments!\n"
	help
elif [ $# == 1 && $1 == **help** || $1 == **--help** ];
then 
	help
fi

map_reduce() {
	if [ $# == 1 ];
	then 
		help
		return 0
	fi

	args=($@)
	for arg in "${args[@]}"
	do
		case $arg in 
			**--data-file**)

				path=$(echo $arg | cut --delimiter='=' --fields=2)
				INPUT_FILE=$(realpath $path);;
			**--ips**)
				path=$(echo $arg | cut --delimiter='=' --fields=2)
				REMOTE_MACHINES_FILE=$(realpath $path);;
			**--project-path**)
				path=$(echo $arg | cut --delimiter='=' --fields=2)
				PROJECT_ROOT=$(realpath $path);;
		esac
	done



	# File containing remote machines
	OUTPUT_FOLDER=$PROJECT_ROOT/output
	SPLITS_FOLDER=$PROJECT_ROOT/splits
	SETUP_FILES_FOLDER=$(dirname "${BASH_SOURCE[0]}")
	SETUP_FILES_FOLDER=$(realpath $SETUP_FILES_FOLDER)/../
	echo $SETUP_FILES_FOLDER

	rm -rf $SPLITS_FOLDER
	mkdir $SPLITS_FOLDER

	rm -rf $OUTPUT_FOLDER 
	mkdir -p $OUTPUT_FOLDER 
	echo "Created ${OUTPUT_FOLDER}"

	#Clean remote computers
	echo "Executing cleaner..."
	local cleaner_results=$(java -jar $SETUP_FILES_FOLDER/Cleaner/target/cleaner-1.0-SNAPSHOT.jar $REMOTE_MACHINES_FILE)
	echo -e $cleaner_results


	# Deploy program to remote machines
	if [ "$cleaner_results" == "[CLEANER] Done for (0) machines!" ];
	then
		return 0
	else
		echo "Executing deployer..."
		java -jar $SETUP_FILES_FOLDER/Deployer/target/deployer-1.0-SNAPSHOT.jar $REMOTE_MACHINES_FILE $INPUT_FILE $PROJECT_ROOT $SETUP_FILES_FOLDER #| grep "TIME-TAKEN"
	fi

	# List files in output directory
	printf "\nFiles created:"
	ls $OUTPUT_FOLDER
}


