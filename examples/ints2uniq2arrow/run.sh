#!/bin/sh

printf '\1\0\0\0\0\0\0\0' > ./sample1.i64.le.dat
printf '\0\1\0\0\0\0\0\0' > ./sample2.i64.le.dat
printf '\0\0\1\0\0\0\0\0' > ./sample3.i64.le.dat

ENV_FILENAME_RAW_INTS_64_LE=./sample1.i64.le.dat ./ints2uniq2arrow
ENV_FILENAME_RAW_INTS_64_LE=./sample2.i64.le.dat ./ints2uniq2arrow
ENV_FILENAME_RAW_INTS_64_LE=./sample3.i64.le.dat ./ints2uniq2arrow
