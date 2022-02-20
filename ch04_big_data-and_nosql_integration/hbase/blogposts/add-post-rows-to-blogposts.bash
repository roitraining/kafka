#! /bin/bash

# use PATH TO FIND HBASE
HBASE_CLI=$(which hbase)

exec $HBASE_CLI shell << EOF
    put 'blogposts', 'post1', 'post:author', 'AMM'
    put 'blogposts', 'post1', 'post:body', 'HTML text'
    put 'blogposts', 'post1', 'post:header', 'header.png'

    put "blogposts", "post2", "post:author", "DBM"
    put "blogposts", "post2", "post:body", "Cooking HTML"
    put "blogposts", "post2", "post:head", "dianne.png"
    put "blogposts", "post2", "post:title", "Cooking Intro"

    put "blogposts", "post3", "post:author", "AMM"
    put "blogposts", "post3", "post:body", "HBase Filters"
    put "blogposts", "post3", "post:head", "header.png"
    put "blogposts", "post3", "post:title", "HBase Filters"

    put "blogposts", "post4", "post:author", "AMM"
    put "blogposts", "post4", "post:body", "HBase PUT"
    put "blogposts", "post4", "post:head", "header.png"
    put "blogposts", "post4", "post:title", "HBase Put"

    put "blogposts", "post5", "post:author", "DBM"
    put "blogposts", "post5", "post:body", "Cooking Pie"
    put "blogposts", "post5", "post:head", "dianne.png"
    put "blogposts", "post5", "post:title", "Cooking Pie"

    put 'blogposts', 'post1', 'post:body', 'HTML text version 2'
    put 'blogposts', 'post1', 'post:body', 'HTML text version 3'
EOF
