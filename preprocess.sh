#! /usr/bin/env bash

tail -n+3 "./input/Partos Casa Materna 16-18_Cleaned.csv" >./input/partos-clean.csv

tail -n+4 "./input/REFERENCIAS 2018.csv" > ./input/refs-clean.csv

# ensure communities is utf-8 with unix endings
vim +"set nobomb | set fenc=utf8 | set ff=unix | x" ./input/communities.txt

