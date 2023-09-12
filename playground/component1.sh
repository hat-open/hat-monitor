#!/bin/sh

. $(dirname -- "$0")/env.sh

exec $PYTHON $RUN_PATH/component.py 127.0.0.1 23010 name1 group
