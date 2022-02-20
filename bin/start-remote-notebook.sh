#! /bin/sh
# https://towardsdatascience.com/how-to-connect-to-jupyterlab-remotely-9180b57c45bb
# jupyter server --generate-config
# jupyter server password
#nohup jupyter lab --allow-root --ip "*" --port 8888 --notebook-dir /class --no-browser > /tmp/error.log &
jupyter lab --allow-root --ip "*" --port 8888 --notebook-dir /class --no-browser 

