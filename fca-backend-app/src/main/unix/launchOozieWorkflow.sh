#!/usr/bin/env sh

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 entry_point" >&2
  exit 1
fi

entryPoint=$1

oozie job -oozie http://${oozieClusterBaseUrl}:11000/oozie -config job.properties -D jump.to=$entryPoint -run