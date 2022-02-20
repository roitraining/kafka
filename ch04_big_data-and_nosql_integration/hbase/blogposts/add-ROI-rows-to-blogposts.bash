#! /bin/bash

# use PATH TO FIND HBASE
HBASE_CLI=$(which hbase)

exec $HBASE_CLI shell << EOF
    put "blogposts", "ROI_post1", "post:author", "DBM", 22
    put "blogposts", "ROI_post1", "post:body", "Cooking HTML", 23
    put "blogposts", "ROI_post1", "post:head", "dianne.png", 24
    put "blogposts", "ROI_post1", "post:title", "Cooking Intro",25

    put "blogposts", "ROI_post2", "post:author", "AMM", 26 
    put "blogposts", "ROI_post2", "post:body", "HBase Filters", 27
    put "blogposts", "ROI_post2", "post:head", "header.png", 28
    put "blogposts", "ROI_post2", "post:title", "HBase Filters", 29

    put "blogposts", "ROI_post3", "post:author", "AMM", 30
    put "blogposts", "ROI_post3", "post:body", "HBase PUT", 31
    put "blogposts", "ROI_post3", "post:head", "header.png", 32
    put "blogposts", "ROI_post3", "post:title", "HBase Put", 33

    put "blogposts", "ROI_post4", "post:author", "MS", 34
    put "blogposts", "ROI_post4", "post:body", "Testing 101", 35
    put "blogposts", "ROI_post4", "post:head", "Testing.img", 36
    put "blogposts", "ROI_post4", "post:title", "Testing 101", 37

    put "blogposts", "TDC_post1", "post:author", "MS", 38
    put "blogposts", "TDC_post1", "post:body", "Testing 102", 39
    put "blogposts", "TDC_post1", "post:head", "crypt.img", 40
    put "blogposts", "TDC_post1", "post:title", "Testing 101", 41

    put "blogposts", "TDC_post2", "post:author", "AMM", 42
    put "blogposts", "TDC_post2", "post:body", "HBase Filters", 43
    put "blogposts", "TDC_post2", "post:head", "crypt.img", 44
    put "blogposts", "TDC_post2", "post:title", "HBase put Filters", 45

    put "blogposts", "TDC_post3", "post:author", "AMM", 46
    put "blogposts", "TDC_post3", "post:body", "HBase PUT", 47
    put "blogposts", "TDC_post3", "post:head", "crypt.img", 48
    put "blogposts", "TDC_post3", "post:title", "HBase scan Filters", 49

    put "blogposts", "TDC_post4", "post:author", "MS", 50
    put "blogposts", "TDC_post4", "post:body", "Writing", 51
    put "blogposts", "TDC_post4", "post:head", "crypt.img", 52
    put "blogposts", "TDC_post4", "post:title", "Cooking Pie", 53

EOF
