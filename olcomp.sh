#!/bin/sh
day=$(date +%Y%m%d)
day_1=$(date -d "yesterday 13:00 " '+%Y%m%d')
day_1_long=$(date -d "yesterday 13:00 " '+%Y-%m-%d')
mkdir $day
cd $day
python /home/lingwei_shu/olprc_compprc_comparison/olcomp.py $day $day_1 $day_1_long

cd ..
