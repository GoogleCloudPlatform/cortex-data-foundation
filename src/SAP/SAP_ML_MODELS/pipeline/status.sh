#!/usr/bin/env bash

logfile="$1"
rc=$(tail -n1 ${logfile} | awk -F\: '{print $2}')

echo "logfile=$logfile"
echo "rc=$rc"

[ "${rc}" -eq 0 ] && exit 0 || exit 1
