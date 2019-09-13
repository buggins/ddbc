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
        -p|--package)
            shift
            package=$1
            ;;
        -h|--help)
            script_name="$(basename $0)"
            echo "Microsoft ODBC setup for Ubuntu Xenial (16.04)"
            echo "Usage: $script_name [-p|--package <PACKAGE>]"
            echo ""
            echo "Options:"
            echo "  -p,--package <PACKAGE>  The version of the ODBC driver to install. eg: msodbcsql17 | msodbcsql"
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

if [ -z "$package" ]; then
    echo -e "Defaulting package to ${BLUE}msodbcsql17${RESET}"
    $package="msodbcsql17"
fi



echo -e "Installing Microsoft ODBC driver: ${CYAN}${package}${RESET}"

__exec curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -

sudo sh -c 'echo "deb [arch=amd64] https://packages.microsoft.com/ubuntu/16.04/prod xenial main" > /etc/apt/sources.list.d/microsoft-prod.list'

MSSRCLST=/etc/apt/sources.list.d/microsoft-prod.list
if [ -f "$MSSRCLST" ]; then
    __exec sudo cat /etc/apt/sources.list.d/microsoft-prod.list
else
    echo -e "${RED}FAILED TO CONFIGURE MICROSOFT REPO${RESET}"
    exit 1
fi

__exec sudo apt-get install apt-transport-https
__exec sudo apt-get update -qq
__exec sudo ACCEPT_EULA=Y apt-get install "${package}" -y

MSLIBDIR="/opt/microsoft/${package}/lib64/"
if [ -d "$MSLIBDIR" ]; then
    echo -e "${GREEN}The ${package} driver is installed to $MSLIBDIR${RESET}"
    __exec odbcinst -q -d
else
    echo -e "${RED}The ${package} driver has not been installed correctly. $MSLIBDIR not found.${RESET}"
    exit 1
fi

#echo -e "${CYAN}Configuring odbcinst.ini file...${RESET}"
#echo -e '[msodbcsql17]\nDescription = MS SQL 2017 Driver\nDriver = /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.4.so.1.1\nTrace = Yes\nTraceFile = /dev/stdout\n' | sudo tee /etc/odbcinst.ini
#__exec odbcinst -i -d -f ./odbcinst.ini
