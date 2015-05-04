#!/bin/bash -u

# Configuration options
# The MySQL master host
master_host=

# The slave host that we want to warm
target_host=

# The workload will be captured pertaining to only this schema if set 
# differently from __all__
schema="__all__"

# The directory where the warmup related data will be stored
working_dir=

# Read-only MySQL user credentials
mysql_username=
mysql_password=

# The MySQL thread concurrency with which to replay the workload
mysql_thd_conc=16

# The duration of seconds for which the workload is captured
# and then replayed
workload_capture_duration_seconds=600


# Setup directories and tools
scripts_root_dir=$(dirname $(readlink -f $0))
workload_logger_script=${scripts_root_dir}/mysql_workload_capture.sh
workload_replay_script=${scripts_root_dir}/mysql_workload_replay.sh
mysqladmin_bin="/usr/bin/mysqladmin"

# Function definitions
function vlog() {
    datetime=$(date "+%Y-%m-%d %H:%M:%S")
    msg="[${datetime}] $1"

    echo ${msg}
}

function show_error_n_exit() {
    error_msg=$1
    echo "ERROR: ${error_msg}"
    exit 1
}

function cleanup() {
    vlog "Doing cleanup before exiting"

    #TODO: add code to cleanup any running ptqd and pt-log-player processes
}

function test_mysql_access() {
#    set -x
    local host=$1
    [[ "${host}" != '' ]] && ${mysqladmin_bin} --host=${host} --user=${mysql_username} --password=${mysql_password} ping >/dev/null 2>&1

    echo $?
#    set +x
}

function setup_directories() {
    vlog "Setting up directory ${working_dir}"
    mkdir -p ${working_dir}
}

function replay_workload() {
#    set -x

    while [ true ]; do
        # Capture the workload
        ${workload_logger_script} --master-host ${master_host} \
            --tcpdump-seconds ${workload_capture_duration_seconds} \
            --output-dir ${working_dir} --schema ${schema}

        # Replay the workload
        ${workload_replay_script} --target-host ${target_host} \
            --slow-log ${working_dir}/mysql.slow.log \
            --output-dir ${working_dir} --mysql-user ${mysql_username} \
            --mysql-password ${mysql_password} --concurrency ${mysql_thd_conc} \
            --cold-run 1

        # Clean up the captured slow log
        rm -f ${working_dir}/mysql.slow.log
    done

#    set +x
}

# Usage info
function show_help() {
cat << EOF
Usage: ${0##*/} --master-host MASTER_HOST --target-host TARGET_HOST --working-dir WORKING_DIR --mysql-user MYSQL_USER --mysql-password MYSQL_PASSWORD [options]
Continously warm up a MySQL slave TARGET_HOST by replaying production workload captured from the production master MASTER_HOST.

Options:

    --help                          display this help and exit
    --master-host MASTER_HOST       the master host actively executing
                                    production traffic that will be
                                    used to capture queries
    --target-host TARGET_HOST       the host that has to be benchmarked
    --working-dir WORKING_DIR       the directory that stores the temporary
                                    data related to slave warmup
    --mysql-user MYSQL_USER         the MySQL read-only username that would
                                    be used to run the queries
    --mysql-password MYSQL_PASSWORD the MySQL read-only user password
    --schema SCHEMA                 capture the workload pertaining to
                                    only this schema (Default: capture
                                    every schema)
EOF
}

function show_help_and_exit() {
    show_help >&2
    exit 22 # Invalid parameters
}

# Command line processing
OPTS=$(getopt -o hm:t:w:u:p:s: --long help,master-host:,target-host:,working-dir:,mysql-user:,mysql-password:,schema: -n 'mysql_slave_warmer.sh' -- "$@")
[ $? != 0 ] && show_help_and_exit

eval set -- "$OPTS"

while true; do
  case "$1" in
    -m | --master-host )        master_host="$2";
                                shift; shift
                                ;;
    -t | --target-host )
                                target_host="$2";
                                shift; shift
                                ;;
    -w | --working-dir )
                                working_dir="$2";
                                shift; shift
                                ;;
    -u | --mysql-user )
                                mysql_username="$2";
                                shift; shift
                                ;;
    -p | --mysql-password )
                                mysql_password="$2";
                                shift; shift
                                ;;

    -s | --schema )             schema="$2";
                                shift; shift
                                ;;
    -h | --help )
                                show_help >&2
                                exit 1
                                ;;
    -- )                        shift; break
                                ;;
    * )
                                show_help >&2
                                exit 1
                                ;;
  esac
done


# Sanity checking of command line parameters
[[ -z ${master_host} ]] && show_help_and_exit >&2

[[ -z ${target_host} ]] && show_help_and_exit >&2

[[ -z ${working_dir} ]] && show_help_and_exit >&2

[[ -z ${mysql_username} ]] && show_help_and_exit >&2

[[ -z ${mysql_password} ]] && show_help_and_exit >&2


# Test that all tools are available
for tool_bin in ${workload_logger_script} ${workload_replay_script} ${mysqladmin_bin}; do
    if (( $(which $tool_bin &> /dev/null; echo $?) != 0 )); then
        echo "Can't find $tool_bin"
        exit 22 # OS error code  22:  Invalid argument
    fi
done

# Test that MySQL credentials are correct
if (( $(test_mysql_access ${target_host}) != 0 )); then
    echo "Could not connect to MySQL on ${target_host}"
    exit 2003
fi

# Do the actual stuff
trap cleanup HUP PIPE INT TERM

# Setup the directories needed on the source and target hosts
setup_directories

# Run the worload capture and replay cycle
replay_workload

# Do the cleanup
cleanup

exit 0
