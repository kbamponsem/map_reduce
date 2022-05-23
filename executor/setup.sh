#!/bin/bash

ARGS=$@
if [ $# -eq 0 ];
then
    printf "\n\tUsage: ./setup.sh -m [[map-reduce] | [sequential] | [clean]]\n\n"
fi

__sequential()
{
    java -Xms2048m -Xmx2048m -jar Sequential/target/Sequential-1.0-SNAPSHOT.jar $1
}

__mvn_setup()
{
    mvn clean && mvn package
}

__get_project_root()
{
    echo $(realpath $(dirname ${BASH_SOURCE[0]}))/../
}

__create_build_dir()
{
    echo -e  "---- [ Creating build directory] ----\n"
    local build_dir=`__get_project_root`/build
    if [ ! -d ${build_dir} ];
    then
        mkdir -p ${build_dir}/jars ${build_dir}/splits ${build_dir}/output
    fi
}

__clean_build_dir()
{
    printf "\033[1m--- [ Cleaning build directory ] ---\n"
    local build_dir=`__get_project_root`/build
    if [ -d ${build_dir} ];
    then
        rm -r ${build_dir}
    fi
}

__copy_jars()
{
    __create_build_dir
    local PROJECT_ROOT=`__get_project_root`
    local jars=${PROJECT_ROOT}/build/jars
    if [ ! -d ${jars} ];
    then
        mkdir ${jars}
    fi
    
    if [ ! -d ${jars}/libs ];
    then
        mkdir ${jars}/libs
    fi
    
    directories=("Cleaner" "Deployer" "CommandRunner" "Mapper" "Reducer" "Shuffler")
    for __dir in ${directories[@]}
    do
        cp ${PROJECT_ROOT}${__dir}/target/*.jar ${jars}
        if [ -d ${PROJECT_ROOT}${__dir}/target/libs ];
        then
            cp ${PROJECT_ROOT}${__dir}/target/libs/* ${jars}/libs
        fi
    done
}


__map_reduce()
{
    if [[ "$#" != 3 ]];
    then
        printf "\n\tUsage: ./setup.sh -m map-reduce [data-file] [remote-ips] [username]\n\n"
        exit 1
    fi
    INPUT_FILE=$(realpath $1)
    PROJECT_ROOT=`__get_project_root`
    REMOTE_MACHINES_FILE=$(realpath $2)
    USERNAME=$3
    BUILD_DIR=${PROJECT_ROOT}/build
    
    echo INPUT FILE $INPUT_FILE
    echo PROJECT ROOT $PROJECT_ROOT
    echo REMOTE MACHINES FILE $REMOTE_MACHINES_FILE
    echo USERNAME ${USERNAME}
    
    # File containing remote machines
    SETUP_FILE=$(realpath "${BASH_SOURCE[0]}")
    
    echo $SETUP_FILE
    
    
    JARS_DIR=${BUILD_DIR}/jars
    
    cp ${REMOTE_MACHINES_FILE} ${BUILD_DIR}
    
    echo "Executing cleaner..."
    java -jar ${JARS_DIR}/cleaner-1.0-SNAPSHOT.jar "${REMOTE_MACHINES_FILE}" "${USERNAME}"
    
    echo "Executing deployer..."
    java -Xms2048m -Xmx2048m -jar ${JARS_DIR}/deployer-1.0-SNAPSHOT.jar "${REMOTE_MACHINES_FILE}" "${INPUT_FILE}" "${PROJECT_ROOT}" "${PROJECT_ROOT}" "${USERNAME}"
}

SWITCH=$1
if [ "${SWITCH}" == "-m" ] || [ "${SWITCH}" == "-M" ];
then
    TYPE=$2
    shift 2
    case "${TYPE}" in
        "map-reduce")
            if [[ ${MAVEN_COMPILE} == 1 ]];
            then
                output=`__mvn_setup | grep ERROR`
                
                if [[ ${output} != "" ]];
                then
                    printf "\n\t---- [ MAVEN FAILED TO COMPILE ] ----\n\n"
                    exit 1
                fi
                __copy_jars `__get_project_root`
            fi
            if [ ! -d `__get_project_root`/build ]
            then
                echo -e "\n\t---- [ Build directory does not exist] ----\n\tPlease run the command with MAVEN_COMPILE=1\n"
                exit 1
            fi
            __map_reduce $@
        ;;
        "sequential")
            __sequential $@
        ;;
        "clean")
            __clean_build_dir
        ;;
        *)
            printf "\n\tUsage: ./setup.sh -m [[map-reduce] | [sequential] | [clean]]\n\n"
        ;;
    esac
fi