#!/bin/bash

if [ -z "$1" ]; then
	echo "Usage: $0 DatasetName"
	echo "The list of files is taken from stdin"
	echo "Example:"
	echo -e "\tls *.root | $0 MyDataset"
	exit
fi

echo [$1]
while read FILE; do
	EVENTS=`edmFileUtil -e file://$FILE | grep "^$FILE" | sed -e "s/.*( \(.*\)events.*/\1/"`
	echo "$PWD/$FILE = $EVENTS"
done
