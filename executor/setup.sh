#!/bin/bash



sequential() 
{
	java -Xms2048m -Xmx2048m -jar Sequential/target/Sequential-1.0-SNAPSHOT.jar $1	
}

map_reduce() 
{

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

	SETUP_FILES_FOLDER=${SETUP_FILE%%/executor/setup.sh}
	SETUP_FILES_FOLDER=$(realpath $SETUP_FILES_FOLDER)
	echo $SETUP_FILES_FOLDER

	rm -rf $SPLITS_FOLDER
	mkdir $SPLITS_FOLDER

	rm -rf $OUTPUT_FOLDER 
	mkdir -p $OUTPUT_FOLDER 
	echo "Created ${OUTPUT_FOLDER}"

	#Clean remote computers
	echo "Executing cleaner..."
	java -jar $SETUP_FILES_FOLDER/Cleaner/target/cleaner-1.0-SNAPSHOT.jar $REMOTE_MACHINES_FILE

	echo "Executing deployer..."
	java -Xms2048m -Xmx2048m -jar $SETUP_FILES_FOLDER/Deployer/target/deployer-1.0-SNAPSHOT.jar $REMOTE_MACHINES_FILE $INPUT_FILE $PROJECT_ROOT $SETUP_FILES_FOLDER | tee >(grep "TIME-TAKEN") >(grep "PROGRAM-TOTAL") > /dev/null

}


