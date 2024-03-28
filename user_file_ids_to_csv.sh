#!/bin/bash

# This script will print all the file ids to a csv file
gam user $1 print filelist id > $1_file_ids.csv
