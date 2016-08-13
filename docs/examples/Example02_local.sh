#!/bin/bash

sleep $1
shift
echo "Hello World - $@"
echo "Hello World - $@" >> argfile

export
export > output.vars1
export > output.vars2
export > output.vars3
