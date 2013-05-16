#!/bin/bash

(
echo "Hallo Welt - $@"
date
echo $FILE_NAMES
) > output.hallo

ls -al $FILE_NAMES

for FILE in $FILE_NAMES; do

echo
echo $FILE
echo

done

export > output.exp
