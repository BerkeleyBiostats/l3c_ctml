docker run  -it --rm \
 -v $(pwd):/home/jovyan/l3c/ \
 -p 8888:8888 \
 -e DISPLAY=$DISPLAY -h $HOSTNAME \
 -v $HOME/.Xauthority:/home/jovyan/.Xauthority \
 l3c-ucb 