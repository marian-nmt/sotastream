#!/bin/bash

# Regression tests for sotastream. Invocation examples:
#  ./run.sh
#  ./run.sh tests/path/to/dir
#  ./run.sh tests/path/to/test_pipeline.sh
#  ./run.sh previous.log

# Environment variables:
#  - SOTASTREAM - path to the root directory of sotastream
#  - DATA - path to the directory with data, default: ./data
#  - PYTHON - path to python command
#  - TIMEOUT - maximum duration for execution of a single test in the format
#    accepted by the timeout command; set to 0 to disable

SHELL=/bin/bash

export LC_ALL=C.UTF-8
export AB_ROOT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

RUN_LOGS="$AB_ROOT/previous.log.tmp"    # Logging file for log and logn commands
rm -f $RUN_LOGS

# Needed so that previous.log is not overwritten when it is provided as an argument
function cleanup {
    test -s "$RUN_LOGS" && mv "$RUN_LOGS" "$AB_ROOT/previous.log"
}
trap cleanup EXIT

function log {
    echo "[$(date '+%m/%d/%Y %T')] $@" | tee -a $RUN_LOGS
}

function logn {
    echo -n "[$(date '+%m/%d/%Y %T')] $@" | tee -a $RUN_LOGS
}

function loge {
    echo $@ | tee -a $RUN_LOGS
}

#####################################################################
log "Running on $(hostname) as process $$"

export AB_PYTHON="${PYTHON:-python3}"
log "Python: $AB_PYTHON"
export AB_SOTASTREAM="$( realpath "${SOTASTREAM:-$AB_ROOT/../../}" )"
log "sotastream dir: $AB_SOTASTREAM"
export AB_DATA="$( realpath ${DATA:-$AB_ROOT/data} )"
log "Data dir: $AB_DATA"

# Add sotastream root directory to PYTHONPATH
export PYTHONPATH+=:$AB_SOTASTREAM

# Time out
export AB_TIMEOUT=${TIMEOUT:-5m}   # the default time out is 5 minutes, see `man timeout`
cmd_timeout=""
if [ $AB_TIMEOUT != "0" ]; then
    cmd_timeout="timeout $AB_TIMEOUT"
fi

log "Time out: $AB_TIMEOUT"


# Exit codes
export EXIT_CODE_SUCCESS=0
export EXIT_CODE_SKIP=100
export EXIT_CODE_TIMEOUT=124    # Exit code returned by the timeout command if timed out

function format_time {
    dt=$(python -c "print($2 - $1)" 2>/dev/null)
    dh=$(python -c "print(int($dt/3600))" 2>/dev/null)
    dt2=$(python -c "print($dt-3600*$dh)" 2>/dev/null)
    dm=$(python -c "print(int($dt2/60))" 2>/dev/null)
    ds=$(python -c "print($dt2-60*$dm)" 2>/dev/null)
    LANG=C printf "%02d:%02d:%02.3fs" $dh $dm $ds
}


###############################################################################
# Default directory with all regression tests
test_prefixes=tests

if [ $# -ge 1 ]; then
    test_prefixes=
    for arg in "$@"; do
        # A log file with paths to test files
        if [[ "$arg" = *.log ]]; then
            # Extract tests from .log file
            args=$(cat $arg | grep -vP '^\[' | grep '/test_.*\.sh' | grep -v '/_' | sed 's/^ *- *//' | tr '\n' ' ' | sed 's/ *$//')
            test_prefixes="$test_prefixes $args"
        # A hash tag
        elif [[ "$arg" = '#'* ]]; then
            # Find all tests with the given hash tag
            tag=${arg:1}
            args=$(find tests -name '*test_*.sh' | xargs -I{} grep -H "^ *# *TAGS:.* $tag" {} | cut -f1 -d:)
            test_prefixes="$test_prefixes $args"
        # A test file or directory name
        else
            test_prefixes="$test_prefixes $arg"
        fi
    done
fi

# Check if the variable is empty or contains only spaces
if [[ -z "${test_prefixes// }" ]]; then
    log "Error: no tests found in the specified input(s): $@"
    exit 1
fi

# Extract all subdirectories, which will be traversed to look for regression tests
test_dirs=$(find $test_prefixes -type d | grep -v "/_" | cat)

if grep -q "/test_.*\.sh" <<< "$test_prefixes"; then
    test_files=$(printf '%s\n' $test_prefixes | sed 's!*/!!')
    test_dirs=$(printf '%s\n' $test_prefixes | xargs -I{} dirname {} | grep -v "/_" | sort | uniq)
fi


###############################################################################
success=true
count_all=0
count_failed=0
count_passed=0
count_skipped=0
count_timedout=0

declare -a tests_failed
declare -a tests_skipped
declare -a tests_timedout

time_start=$(date +%s.%N)

# Traverse test directories
cd $AB_ROOT
for test_dir in $test_dirs
do
    log "Checking directory: $test_dir"
    nosetup=false

    # Run setup script if exists
    if [ -e $test_dir/setup.sh ]; then
        log "Running setup script"

        cd $test_dir
        $cmd_timeout $SHELL -v setup.sh &> setup.log
        if [ $? -ne 0 ]; then
            log "Warning: setup script returns a non-success exit code"
            success=false
            nosetup=true
        else
            rm setup.log
        fi
        cd $AB_ROOT
    fi

    # Run tests
    for test_path in $(ls -A $test_dir/test_*.sh 2>/dev/null)
    do
        test_file=$(basename $test_path)
        test_name="${test_file%.*}"

        # In non-traverse mode skip tests if not requested
        if [[ -n "$test_files" && $test_files != *"$test_file"* ]]; then
            continue
        fi
        test_time_start=$(date +%s.%N)
        ((++count_all))

        # Tests are executed from their directory
        cd $test_dir

        # Skip tests if setup failed
        logn "Running $test_path ... "
        if [ "$nosetup" = true ]; then
            ((++count_skipped))
            tests_skipped+=($test_path)
            loge " skipped"
            cd $AB_ROOT
            continue;
        fi

        # Run test
        # Note: all output gets written to stderr (very very few cases write to stdout)
        $cmd_timeout $SHELL -x $test_file 2> $test_file.log 1>&2
        exit_code=$?

        # Check exit code
        if [ $exit_code -eq $EXIT_CODE_SUCCESS ]; then
            ((++count_passed))
            loge " OK"
        elif [ $exit_code -eq $EXIT_CODE_SKIP ]; then
            ((++count_skipped))
            tests_skipped+=($test_path)
            loge " skipped"
        elif [ $exit_code -eq $EXIT_CODE_TIMEOUT ]; then
            ((++count_timedout))
            tests_timedout+=($test_path)
            # Add a comment to the test log file that it timed out
            echo "The test timed out after $TIMEOUT" >> $test_file.log
            # A timed out test is a failed test
            ((++count_failed))
            loge " timed out"
            success=false
        else
            ((++count_failed))
            tests_failed+=($test_path)
            loge " failed"
            success=false
        fi

        # Report time
        test_time_end=$(date +%s.%N)
        test_time=$(format_time $test_time_start $test_time_end)
        log "Test took $test_time"

        cd $AB_ROOT
    done
    cd $AB_ROOT

    # Run teardown script if exists
    if [ -e $test_dir/teardown.sh ]; then
        log "Running teardown script"

        cd $test_dir
        $cmd_timeout $SHELL teardown.sh &> teardown.log
        if [ $? -ne 0 ]; then
            log "Warning: teardown script returns a non-success exit code"
            success=false
        else
            rm teardown.log
        fi
        cd $AB_ROOT
    fi
done

time_end=$(date +%s.%N)
time_total=$(format_time $time_start $time_end)


###############################################################################
# Print skipped and failed tests
if [ -n "$tests_skipped" ] || [ -n "$tests_failed" ] || [ -n "$tests_timedout" ]; then
    loge "---------------------"
fi
[[ -z "$tests_skipped" ]] || loge "Skipped:"
for test_name in "${tests_skipped[@]}"; do
    loge "- $test_name"
done
[[ -z "$tests_failed" ]] || loge "Failed:"
for test_name in "${tests_failed[@]}"; do
    loge "- $test_name"
done
[[ -z "$tests_timedout" ]] || loge "Timed out:"
for test_name in "${tests_timedout[@]}"; do
    loge "- $test_name"
done
[[ -z "$tests_failed" ]] || echo "Logs:"
for test_name in "${tests_failed[@]}"; do
    echo "- $(realpath $test_name | sed 's/\.sh/.sh.log/')"
done


###############################################################################
# Print summary
loge "---------------------"
loge -n "Ran $count_all tests in $time_total, $count_passed passed, $count_skipped skipped, $count_failed failed"
[ -n "$tests_timedout" ] && loge -n " (incl. $count_timedout timed out)"
loge ""

# Return exit code
if $success && [ $count_all -gt 0 ]; then
    loge "OK"
    exit 0
else
    loge "FAILED"
    exit 1
fi
