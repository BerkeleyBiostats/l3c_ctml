#!/bin/bash

CID=$(docker run -v $(pwd):/home/jovyan/l3c/ -d l3c-ucb tail -f /dev/null)
echo "started container $CID"
echo "run exit to exit and stop container"
docker exec -u jovyan -it $CID bash
docker stop $CID
docker rm $CID
echo "stopped container $CID"


