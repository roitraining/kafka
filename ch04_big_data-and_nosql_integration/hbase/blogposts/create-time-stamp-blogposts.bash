#! /bin/bash

# use PATH TO FIND HBASE
HBASE_CLI=$(which hbase)

exec $HBASE_CLI shell << EOF
    require 'stringio'
    foo = StringIO.new
    \$stdout = foo
    exists "blogposts"
    exists_stdout = \$stdout.string
    \$stdout = STDOUT

    if not exists_stdout.include? 'not'
       disable "blogposts"
       drop "blogposts"
    end
    
    create "blogposts", { NAME => "post", VERSIONS => 3 } 

    put "blogposts", "post1", "post:author", "AMM", 1
    put "blogposts", "post1", "post:body", "HBase Filters", 2
    put "blogposts", "post1", "post:head", "header.png", 3
    put "blogposts", "post1", "post:title", "HBase Filters", 4

    put "blogposts", "post1", "post:body", "HBase Filters v2 ", 5
EOF
