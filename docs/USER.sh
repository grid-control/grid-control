#!/bin/bash

(
echo "Hallo Welt - $@"
date
) > output.date

export > output.vars
cat USER-input.txt > output.subst
