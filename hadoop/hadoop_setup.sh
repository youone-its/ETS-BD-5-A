#!/bin/bash

#bikin direktori buat weatherpulse nya
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/weather/api
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/weather/rss
docker exec -it hadoop-namenode hdfs dfs -mkdir -p /data/weather/hasil

#cek direktori
docker exec -it hadoop-namenode hdfs dfs -ls -R /data/

#cek datanode
docker exec -it hadoop-namenode hdfs dfsadmin -report

#cek yarn
docker exec -it hadoop-namenode yarn node -list

