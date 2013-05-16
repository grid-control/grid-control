#!/bin/bash

cat

(
echo "Hallo Welt - $@"
date
) > output.date

export > output.vars
cat USERMOD-parameter-input.txt > output.subst

echo "SE TEST1" > SE1
echo "SE TEST2" > SE2

cat IN*

echo "HALLO"
echo "WELT" >&2
echo $x $y $z $spam
echo $TEST