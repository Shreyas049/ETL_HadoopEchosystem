#!/bin/bash

if ! rm -r /home/theshree/streamingData/*
then
    echo "file exists"
fi

if ! hdfs dfs -rm -r /miniproject/raw/dq_good
then
  echo "dq_good not present"
fi

if ! hdfs dfs -rm -r /miniproject/raw/persist/*
then
  echo "persist empty"
fi