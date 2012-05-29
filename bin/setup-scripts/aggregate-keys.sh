#!/bin/bash

set -e

if [ "$USER" != 'root' ]; then
    echo "You definitely must be root to pull this off." >&2
    exit 1
fi

USERS="$1"
TARGET_DIR=/tmp/kraken-keys/$HOSTNAME

echo "Aggregating keys...  [users: $USERS; target: $TARGET_DIR]"

for u in $USERS; do
    mkdir -p $TARGET_DIR/$u
    cp -v /home/$u/.ssh/id_rsa* $TARGET_DIR/$u/
done

echo "word."
