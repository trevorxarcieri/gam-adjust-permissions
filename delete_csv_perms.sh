#!/bin/bash

# Check if the first argument is empty. If so, set it to "all"
if [ -z "$1" ]; then
  filename="all"
else
  filename="$1"
fi

# Use GNU Parallel to process each line in parallel
parallel -d "\r\n" --skip-first-line --colsep ',' gam user {1} delete permission id {2} {3} :::: "${filename}_file_ids_and_permIds.csv"

