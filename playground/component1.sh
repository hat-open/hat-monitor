#!/bin/sh

set -e

PLAYGROUND_PATH=$(dirname "$(realpath "$0")")
. $PLAYGROUND_PATH/env.sh

exec $PYTHON $PLAYGROUND_PATH/component.py 127.0.0.1 23010 name1 group
