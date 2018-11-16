#! /usr/bin/env bash

# Couldn't get automatic conversion to CSV working

#function make_csv {
    #libreoffice --headless --convert-to csv $1 --outdir $2
#}

#make_csv input/Partos* input/partos-raw.csv
#make_csv input/REFERENCIAS* input/refs-raw.csv
#make_csv input/SM_Database* input/censo.csv

tail -n+3 "./input/partos-raw.csv" >./input/partos-clean.csv

tail -n+4 "./input/refs-raw.csv" > ./input/refs-clean.csv

# ensure communities is utf-8 with unix endings
vim +"set nobomb | set fenc=utf8 | set ff=unix | x" ./input/communities.txt

