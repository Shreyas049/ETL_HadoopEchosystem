#!/bin/bash

hdfs dfs -rm -r /miniproject/raw/stage/*
hdfs dfs -rm -r /miniproject/checkpoint/*
rm -r /home/theshree/streamingData/*

hdfs dfs -rm -r /miniproject/raw/dq_good
hdfs dfs -rm -r /miniproject/raw/persist/*