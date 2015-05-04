#!/bin/bash -u

# Configuration options
# The prod master that will be used to capture the prod workload
master_host=

# The amount of seconds up to which tcpdump must be run to capture the queries
tcpdump_time_limit_sec=

# The directory on the host running the script, that will store the tcpdump data
output_dir=

# The workload will be captured pertaining to only this schema if set 
# differently from __all__
schema="__all__"


mysql_interface=eth0
mysql_port=3306
nc_port=7778

# Setup file prefixes
tcpdump_filename=mysql.tcp
ptqd_filename=ptqd.txt
ptqd_slowlog_name=mysql.slow.log

# Setup tools
tcpdump_bin="/usr/sbin/tcpdump"
nc_bin="/usr/bin/nc"
pt_query_digest_bin="/usr/bin/pt-query-digest"

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

    # Cleanup any outstanding netcat sockets
    cleanup_nc ${nc_port}

    #TODO: add cleanup code to cleanup any running tcpdump processes
}

function get_nc_pid() {
#    set -x
    local port=$1

    local pid=$(ps -aef | grep nc | grep -v bash | grep ${port} | awk '{ print $2 }')
    echo ${pid}
#    set +x
}

function cleanup_nc() {
    local port=$1

    local pid=$(get_nc_pid ${port})
    vlog "Killing nc pid ${pid}"

    [[ "${pid}" != '' && "${pid}" != 0 ]] && kill ${pid} && (kill ${pid} && kill -9 ${pid}) || :
}

function check_pid() {
#    set -x
    local pid=$1
    [[ "${pid}" != 0 && "${pid}" != '' ]] && ps -p ${pid} >/dev/null 2>&1

    echo $?
#    set +x
}

# waits ~10 seconds for nc to open the port and then reports ready
function wait_for_nc()
{
#    set -x
    local port=$1

    for i in $(seq 1 50); do
        netstat -nptl 2>/dev/null | grep '/nc\s*$' | awk '{ print $4 }' | \
            sed 's/.*://' | grep \^${port}\$ >/dev/null && break
        sleep 0.2
    done

    vlog "ready localhost:${port}"
#    set +x
}

function setup_directories() {
    vlog "Initializing directories"

    # Initialize directory that will store the tcpdump data
    mkdir -p ${output_dir}
}

function get_tcpdump_from_master() {
#    set -x

    local tcpdump_args="-i ${mysql_interface} -B 10000 -s 65535 -x -n -q -tttt 'port ${mysql_port} and tcp[1] & 7 == 2 and tcp[3] & 7 == 2'"
    local tcpdump_file="${output_dir}/${tcpdump_filename}"
    local slowlog_file="${output_dir}/${ptqd_slowlog_name}"
    local this_host=$(hostname)

    vlog "Starting to capture production queries via tcpdump on the master ${master_host}"

    # Check if the nc port is already being used
    # We abort in case its already being used
    local nc_pid=$(get_nc_pid ${nc_port})
    if [[ ! -z ${nc_pid} && $(check_pid ${nc_pid}) == 0 ]]; then 
        display_error_n_exit "Could not create the socket localhost:${nc_port}, port already in use."
    fi

    # We start the wait thread before hand, this will watch for the nc listen socket to be created
    wait_for_nc ${nc_port} &

    # Create receiving socket
    vlog "Creating receiving socket on localhost"
    nohup bash -c "($nc_bin -dl $nc_port > ${tcpdump_file}) &" > /dev/null 2>&1
    
    wait %% # join wait_for_nc thread

    # check if nc is running, if not then it errored out
    local nc_pid=$(get_nc_pid ${nc_port})
    (( $(check_pid ${nc_pid}) != 0 )) && display_error_n_exit "Could not create the socket localhost:${nc_port}"

    vlog "Capturing MySQL workload on ${master_host} via tcpdump for ${tcpdump_time_limit_sec} seconds"
    vlog "Executing ${tcpdump_bin} ${tcpdump_args} on ${master_host}"
    ssh -t ${master_host} "sudo timeout ${tcpdump_time_limit_sec} ${tcpdump_bin} ${tcpdump_args} | ${nc_bin} ${this_host} ${nc_port}"

    # Below is a temp solution for the bug 
    # https://bugs.launchpad.net/percona-toolkit/+bug/1402776
    #sed 's/\x0mysql_native_password//g' ${slowlog_file}

    if [[ "${schema}" == "__all__" ]]; then
        ${pt_query_digest_bin} --type tcpdump ${tcpdump_file} \
            --output slowlog --no-report \
            --filter '(defined $event->{db}) && ($event->{fingerprint} =~ m/^select/i) && ($event->{arg} !~ m/FOR UPDATE/i) && ($event->{arg} !~ m/LOCK IN SHARE MODE/i)' \
            | sed -e 's/\x0mysql_native_password//g' -e 's/# Client: /# User@Host: /g' > ${slowlog_file} 2> /dev/null
    else
        ${pt_query_digest_bin} --type tcpdump ${tcpdump_file} \
            --output slowlog --no-report \
            --filter '(($event->{db} || "") =~ m/^'${schema}'/) && ($event->{fingerprint} =~ m/^select/i) && ($event->{arg} !~ m/FOR UPDATE/i) && ($event->{arg} !~ m/LOCK IN SHARE MODE/i)' \
            | sed -e 's/\x0mysql_native_password//g' -e 's/# Client: /# User@Host: /g' > ${slowlog_file} 2> /dev/null
    fi

    # Remove the tcpdump file as we have parsed it into a slow log file
    rm -f ${tcpdump_file}

    vlog "MySQL workload successfully streamed from ${master_host} to ${slowlog_file}"

#    set +x
}

# Usage info
function show_help() {
cat << EOF
Usage: ${0##*/} --master-host MASTER_HOST --tcpdump-seconds TCPDUMP_TIME_LIMIT_SEC --output-dir OUTPUTDIR [options]
Capture tcpdump output from MASTER_HOST and stream it to OUTPUTDIR.

Options:

    --help                                   display this help and exit
    --master-host MASTER_HOST                the master host actively executing
                                             production traffic that will be
                                             used to capture queries via
                                             tcpdump
    --tcpdump-seconds TCPDUMP_TIME_LIMIT_SEC the number of seconds for which
                                             tcpdump will be run on MASTER_HOST
    --output-dir OUTPUTDIR                   the directory on that will be used
                                             for storing the tcpdump file
    --port PORT                              the port on localhost where the
                                             captured workload from master is
                                             received (Default: 7778)
    --schema SCHEMA                          capture the workload pertaining to
                                             only this schema (Default: capture
                                             every schema)
EOF
}

function show_help_and_exit() {
    show_help >&2
    exit 22 # Invalid parameters
}

# Command line processing
OPTS=$(getopt -o hm:l:o:p:s: --long help,master-host:,tcpdump-seconds:,output-dir:,port:,schema: -n 'mysql_workload_capture.sh' -- "$@")
[ $? != 0 ] && show_help_and_exit

eval set -- "$OPTS"

while true; do
  case "$1" in
    -m | --master-host )
                                master_host="$2";
                                shift; shift
                                ;;
    -l | --tcpdump-seconds )
                                tcpdump_time_limit_sec="$2";
                                shift; shift
                                ;;
    -o | --output-dir )
                                output_dir="$2";
                                shift; shift
                                ;;
    -p | --port )
                                nc_port="$2";
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

ssh -q ${master_host} "exit"
(( $? != 0 )) && show_error_n_exit "Could not SSH into ${master_host}"

[[ -z ${tcpdump_time_limit_sec} ]] && show_help_and_exit >&2

[[ -z ${output_dir} ]] && show_help_and_exit >&2


# Test that all tools are available on remote host
if (( $(ssh ${master_host} "which $tcpdump_bin" &> /dev/null; echo $?) != 0 )); then
    echo "Can't find $tcpdump_bin on ${master_host}"
    exit 22 # OS error code  22:  Invalid argument
fi

for tool_bin in ${nc_bin}; do
    if (( $(ssh ${master_host} "which $tool_bin" &> /dev/null; echo $?) != 0 )); then
        echo "Can't find $tool_bin on ${master_host}"
        exit 22 # OS error code  22:  Invalid argument
    fi
done

# Test that all tools are available locally
for tool_bin in ${nc_bin} ${pt_query_digest_bin}; do
    if (( $(which $tool_bin &> /dev/null; echo $?) != 0 )); then
        echo "Can't find $tool_bin on localhost"
        exit 22 # OS error code  22:  Invalid argument
    fi
done


# Do the actual stuff
trap cleanup HUP PIPE INT TERM

# Setup the directories needed on the source and target hosts
setup_directories

# Capture and transfer tcpdump from source to target host
get_tcpdump_from_master

# Do the cleanup
cleanup

exit 0
