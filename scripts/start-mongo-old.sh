#! /bin/sh
[! -d "/class/mongodb"] && mkdir /class/mongodb
mongod --dbpath /class/mongodb &


