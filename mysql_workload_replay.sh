#!/bin/bash -u

# Configuration options
# The host that we want to benchmark to guage performance
target_host=

# The directory where the benchmark related data will be stored
output_dir=

# Read-only MySQL user credentials
mysql_username=
mysql_password=

# Should the benchmark be run with cold InnoDB Buffer Pool cache. When this
# is enabled then the Buffer Pool is not warmed up before replaying the
# workload. This can be important in cases where you want to test MySQL
# performance with cold caches
benchmark_cold_run=0

# The MySQL thread concurrency used to run the benchmark. Ideally this is
# the same concurrency as the workload running on the master.
mysql_thd_conc=6


# Setup tools
mysqladmin_bin="/usr/bin/mysqladmin"
mysql_bin="/usr/bin/mysql"
percona_playback_bin="/usr/bin/percona-playback"

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
    vlog "Setting up directory ${output_dir}"
    mkdir -p ${output_dir}
}

function run_benchmark() {
#    set -x

    # Prepare the directories used by percona-playback
    local tmp_dir="${output_dir}/tmp"
    mkdir -p ${tmp_dir}

    # Warm up the buffer pool on the target host
    if [[ "${benchmark_cold_run}" == "0" ]]; then
        vlog "Warming up the buffer pool on the host ${target_host}"
        ${percona_playback_bin} --db-plugin=libmysqlclient \
            --query-log-file=${slowlog_file} --loop=3 \
            --dispatcher-plugin=thread-pool --thread-pool-threads-count=${mysql_thd_conc} \
            --mysql-host=${target_host} --mysql-username=${mysql_username} \
            --mysql-password=${mysql_password} &> /dev/null
    fi

    # Run the benchmark against the target host
    vlog "Starting to run the benchmark on the target host ${target_host} with a max concurrency of ${mysql_thd_conc}"
    ${percona_playback_bin} --db-plugin=libmysqlclient \
        --query-log-file=${slowlog_file} --query-log-preserve-query-time\
        --dispatcher-plugin=thread-pool --thread-pool-threads-count=${mysql_thd_conc} \
        --mysql-host=${target_host} --mysql-username=${mysql_username} \
        --mysql-password=${mysql_password} \
        > ${tmp_dir}/playback.log 2> ${tmp_dir}/playback.err

        #--session-init-query="SET long_query_time=0" \

    vlog "Benchmarks completed."

#    set +x
}

function print_benchmark_results() {
    echo
    echo "Queries benchmark summary from the target ${target_host}"
    awk '/user time,/,/# Query size/' ${output_dir}/ptqd.${target_host}.txt | grep -v "# Files:" | grep -v "# Hostname:"

    echo
    echo "Detailed reports are available at ${output_dir}"
    echo "###########################################################################"
}

# Usage info
function show_help() {
cat << EOF
Usage: ${0##*/} --target-host TARGET_HOST --slow-log SLOW_LOG --output-dir OUTPUT_DIR --mysql-user MYSQL_USER --mysql-password MYSQL_PASSWORD [options]
Replay MySQL production workload in slowlog format on TARGET_HOST and report the query times.

Options:

    --help                          display this help and exit
    --target-host TARGET_HOST       the host that has to be benchmarked
    --slow-log SLOW_LOG             the slow log file containing the workload
                                    that needs to be replayed
    --output-dir OUTPUT_DIR         the directory that stores the benchmark
                                    reports
    --mysql-user MYSQL_USER         the MySQL read-only username that would
                                    be used to run the queries
    --mysql-password MYSQL_PASSWORD the MySQL read-only user password
    --concurrency CONCURRENCY       the MySQL thread concurrency at which to
                                    run the benchmark (default 6)
    --cold-run                      run the benchmark with cold InnoDB Buffer
                                    Pool cache, this is disabled by default
EOF
}

function show_help_and_exit() {
    show_help >&2
    exit 22 # Invalid parameters
}

# Command line processing
OPTS=$(getopt -o hct:s:o:u:p:C: --long help,cold-run,target-host:,slow-log:,output-dir:,mysql-user:,mysql-password:,concurrency: -n 'mysql_workload_replay.sh' -- "$@")
[ $? != 0 ] && show_help_and_exit

eval set -- "$OPTS"

while true; do
  case "$1" in
    -t | --target-host )
                                target_host="$2";
                                shift; shift
                                ;;
    -s | --slow-log )           slowlog_file="$2"
                                shift; shift
                                ;;
    -o | --output-dir )
                                output_dir="$2";
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

    -C | --concurrency)         mysql_thd_conc="$2";
                                shift; shift
                                ;;

    -c | --cold-run )           benchmark_cold_run=1
                                shift;
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
[[ -z ${target_host} ]] && show_help_and_exit >&2

[[ -z ${slowlog_file} || ! -f ${slowlog_file} || ! -s ${slowlog_file} ]] && show_help_and_exit >&2

[[ -z ${output_dir} ]] && show_help_and_exit >&2

[[ -z ${mysql_username} ]] && show_help_and_exit >&2

[[ -z ${mysql_password} ]] && show_help_and_exit >&2


# Test that all tools are available
for tool_bin in ${percona_playback_bin}; do
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

# Do the benchmark run
run_benchmark

# Print the benchmark report at the end
#print_benchmark_results

# Do the cleanup
cleanup

exit 0
