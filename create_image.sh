#!/bin/bash
echo 'Creating Image'$1
docker build -t $1 .
