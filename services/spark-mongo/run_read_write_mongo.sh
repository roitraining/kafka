#!/bin/bash
docker-compose up -d
docker-compose exec work-env python read_write_mongo.py
