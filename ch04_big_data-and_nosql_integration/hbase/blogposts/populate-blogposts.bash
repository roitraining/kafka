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

    put 'blogposts', 'post1', 'post:author', 'AMM'
    put 'blogposts', 'post1', 'post:body', 'HTML text'
    put 'blogposts', 'post1', 'post:head', 'header.png'
    put "blogposts", "post1", "post:title", "HBase Filters"

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

    put "blogposts", "ROI_post1", "post:author", "DBM"
    put "blogposts", "ROI_post1", "post:body", "Cooking HTML"
    put "blogposts", "ROI_post1", "post:head", "dianne.png"
    put "blogposts", "ROI_post1", "post:title", "Cooking Intro"

    put "blogposts", "ROI_post2", "post:author", "AMM"
    put "blogposts", "ROI_post2", "post:body", "HBase Filters"
    put "blogposts", "ROI_post2", "post:head", "header.png"
    put "blogposts", "ROI_post2", "post:title", "HBase Filters"

    put "blogposts", "ROI_post3", "post:author", "AMM"
    put "blogposts", "ROI_post3", "post:body", "HBase PUT"
    put "blogposts", "ROI_post3", "post:head", "header.png"
    put "blogposts", "ROI_post3", "post:title", "HBase Put"

    put "blogposts", "ROI_post4", "post:author", "MS"
    put "blogposts", "ROI_post4", "post:body", "Testing 101"
    put "blogposts", "ROI_post4", "post:head", "Testing.img"
    put "blogposts", "ROI_post4", "post:title", "Testing 101"

    put "blogposts", "TDC_post1", "post:author", "MS"
    put "blogposts", "TDC_post1", "post:body", "Testing 102"
    put "blogposts", "TDC_post1", "post:head", "crypt.img"
    put "blogposts", "TDC_post1", "post:title", "Testing 101"

    put "blogposts", "TDC_post2", "post:author", "AMM"
    put "blogposts", "TDC_post2", "post:body", "HBase Filters"
    put "blogposts", "TDC_post2", "post:head", "crypt.img"
    put "blogposts", "TDC_post2", "post:title", "HBase put Filters"

    put "blogposts", "TDC_post3", "post:author", "AMM"
    put "blogposts", "TDC_post3", "post:body", "HBase PUT"
    put "blogposts", "TDC_post3", "post:head", "crypt.img"
    put "blogposts", "TDC_post3", "post:title", "HBase scan Filters"


    put "blogposts", "TDC_post4", "post:author", "MS"
    put "blogposts", "TDC_post4", "post:body", "Writing"
    put "blogposts", "TDC_post4", "post:head", "crypt.img"
    put "blogposts", "TDC_post4", "post:title", "Cooking Pie"

EOF
