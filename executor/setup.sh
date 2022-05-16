#!/bin/bash

ARGS=$@
if [ $# -eq 0 ];
then
	printf "Usage: ./setup.sh -m [map-reduce], [sequential]"
fi

__sequential() 
{
	java -Xms2048m -Xmx2048m -jar Sequential/target/Sequential-1.0-SNAPSHOT.jar $1	
}

__mvn_setup()
{
	mvn clean && mvn package
}

__map_reduce() 
{
	if [[ "$#" != 3 ]];
	then
		printf "Usage: ./setup.sh -m map-reduce [data-file] [output_dir] [remote-ips]\n"
		exit 1
	fi
	INPUT_FILE=$(realpath $1)
	PROJECT_ROOT=$(realpath $2)
	REMOTE_MACHINES_FILE=$(realpath $3)

	echo INPUT FILE $INPUT_FILE
	echo PROJECT ROOT $PROJECT_ROOT 
	echo REMOTE MACHINES FILE $REMOTE_MACHINES_FILE


	# File containing remote machines
	OUTPUT_FOLDER=$PROJECT_ROOT/output
	SPLITS_FOLDER=$PROJECT_ROOT/splits
	SETUP_FILE=$(realpath "${BASH_SOURCE[0]}")

	echo $SETUP_FILE

	SETUP_FILES_FOLDER=$(realpath $(dirname ${BASH_SOURCE[0]}))
	echo $SETUP_FILES_FOLDER

	rm -rf $SPLITS_FOLDER
	mkdir $SPLITS_FOLDER

	rm -rf $OUTPUT_FOLDER 
	mkdir -p $OUTPUT_FOLDER 
	echo "Created ${OUTPUT_FOLDER}"
	echo ${BASE_SOURCE[0]}
	#Clean remote computers
	echo "Executing cleaner..."
	java -jar $SETUP_FILES_FOLDER/../Cleaner/target/cleaner-1.0-SNAPSHOT.jar $REMOTE_MACHINES_FILE

	echo "Executing deployer..."
	java -Xms2048m -Xmx2048m -jar $SETUP_FILES_FOLDER/../Deployer/target/deployer-1.0-SNAPSHOT.jar $REMOTE_MACHINES_FILE $INPUT_FILE $PROJECT_ROOT $SETUP_FILES_FOLDER | tee >(grep "TIME-TAKEN") >(grep "PROGRAM-TOTAL") > /dev/null
}

SWITCH=$1
if [ $SWITCH == "-m" ] || [ $SWITCH == "-M" ];
then
	TYPE=$2
	shift 2
	case "${TYPE}" in
		"map-reduce")
			__mvn_setup
			__map_reduce $@
		;;
		"sequential")
			__sequential $@
		;;
	esac
fi