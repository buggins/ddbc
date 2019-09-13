#!/usr/bin/env sh
set -e

RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
BLUE="\e[34m"
MAGENTA="\e[35m"
CYAN="\e[36m"
RESET="\e[0m"

# functions
__exec() {
    local cmd=$1
    shift

    local cmdname=$(basename $cmd)
    # put on stderr to avoid it being captured in variables
    echo -e "${CYAN}> $cmdname $@${RESET}" >&2

    $cmd $@

    local exitCode=$?
    if [ $exitCode -ne 0 ]; then
        echo -e "${RED}'$cmdname $@' failed with exit code $exitCode${RESET}" 1>&2
        exit $exitCode
    fi
}

# main

while [ $# -ne 0 ]
do
    arg_val=$1
    case $arg_val in
        -n|--name)
            shift
            instance_name=$1
            ;;
        -p|--sa-password)
            shift
            sa_password=$1
            ;;
        -v|--version)
            shift
            version=$1
            ;;
        -h|--help)
            script_name="$(basename $0)"
            echo "Setup Microsoft SQL Server"
            echo "Usage: $script_name [-n|--name <NAME>] [-p|--sa-password <SA_PASSWORD>] [-v|--version <VERSION>]"
            echo ""
            echo "Options:"
            echo "  -n,--name <NAME>  The name for the docker instance"
            echo "  -p,--sa-password <SA_PASSWORD>  The password to set for the 'sa' user on the server"
            echo "  -v,--version <VERSION>  The version of the SQL Server image. eg: 2017-latest-ubuntu | 2019-CTP3.2-ubuntu"
            echo "  -h,--help                       Shows this help message"
            exit 0
            ;;
        *)
            say_err "Unknown argument \`$arg_val\`"
            exit 1
            ;;
    esac

    shift
done

if [ -z "$instance_name" ]; then
    echo -e "Defaulting instance name to ${BLUE}mssql${RESET}"
    $instance_name=mssql
fi

if [ -z "$version" ]; then
    echo -e "Defaulting MS SQL Server image version to ${BLUE}latest${RESET}"
    $version=latest
fi

if [ -z "$sa_password" ]; then
    echo -e "${RED}Required option -p|--sa-password is not set. Run --help to see usage.${RESET}"
    exit 1
fi

__exec docker pull mcr.microsoft.com/mssql/server:$version
container=$(__exec docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=$sa_password" -p 1433:1433 --name "$instance_name" -d mcr.microsoft.com/mssql/server:$version)
echo "Created container $container"
__exec docker ps -a

# wait long enough for docker to start the container and check if enough memory is available
# mssql requries at least 4 GB of memory
dbserver=localhost
dbport=1433
retries=20
__exec sleep 5s
until nc -z $dbserver $dbport
do
    echo "$(date) - waiting for ${dbserver}:${dbport}..."
    if [ "$retries" -le 0 ]; then
        echo "${RED}Done waiting. There might have been a problem starting the server.${RESET}"
        __exec docker logs $container
        exit 1
    fi
    retries=$((retries - 1))
    echo "Waiting before retrying. Retries left: $retries"
    sleep 5s
done

echo -e "${GREEN}MS SQL Server is ready${RESET}"
