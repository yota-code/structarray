#!/usr/bin/env zsh

if [[ ! -f ${1}.c ]]
then
    echo "No such file"
fi

readelf -wi ${1}.exe > debug_info.log

gcc -g ${1}.c -o ${1}.exe

./run.py ${1}.exe

