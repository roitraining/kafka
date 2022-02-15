#! /bin/sh
nohup jupyter lab --allow-root --ip "*" --notebook-dir /class --no-browser > /tmp/error.log &

