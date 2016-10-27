#!/bin/bash
echo 'Deleting Unused Containers...'
docker rm $(docker ps -a -q)
echo 'Deleting Unused Images....'
docker rmi $(docker images -a -q)
