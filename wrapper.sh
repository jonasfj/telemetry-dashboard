#!/bin/bash


tar -xzf boto.tar.gz
export PYTHONPATH=$PWD/;

set -o pipefail

read part
part=`echo $part | cut -d " " -f 2`

hdfs dfs -text hdfs:///telemetry_dump/$part | python process_seq_file-fast.py $part.result

if [ $? -eq 0 ]; then

  python put.py $part.result && rm $part.result;

  if [ $? -eq 0 ]; then
    echo "SUCCESS $part";
  else
    echo "FAILED $part";
  fi
else
  echo "FAILED $part";
fi
