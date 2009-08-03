#!/bin/bash

(
echo "Hallo Welt - $@"
date
) > output.date

export > output.vars
cat USER-input.txt > output.subst

echo "SE TEST1" > SE1
echo "SE TEST2" > SE2
