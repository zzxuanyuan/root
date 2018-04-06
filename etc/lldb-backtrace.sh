#!/bin/sh

# This script is used by TUnixSystem::StackTrace() on MacOS X if lldb is supported.

tempname=`basename $0 .sh`

OUTFILE=/dev/stdout

if [ `uname -s` != "Darwin" ]; then

   echo "lldb stacktrace is only supported on MacOSX."
   exit 1

else

   if test $# -lt 1; then
      echo "Usage: ${tempname} <process-id>" 1>&2
      exit 1
   fi

   TMPFILE=`mktemp -q /tmp/${tempname}.XXXXXX`
   if test $? -ne 0; then
      echo "${tempname}: can't create temp file, exiting..." 1>&2
      exit 1
   fi

   backtrace="thread backtrace all"

   echo $backtrace > $TMPFILE

   LLDB=${LLDB:-lldb}

   # Run LLDB, strip out unwanted noise.
   $LLDB -b -x -s $TMPFILE -p $1 2>&1  < /dev/null |
   sed -n \
    -e '/[Tt]hread #[0-9]/p' \
    -e '/[Ff]rame #[0-9]/p' > $OUTFILE

   rm -f $TMPFILE

fi
