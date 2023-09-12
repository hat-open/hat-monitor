#!/bin/sh

. $(dirname -- "$0")/env.sh

exec $PYTHON $RUN_PATH/component.py 127.0.0.1 24010 name2 group
