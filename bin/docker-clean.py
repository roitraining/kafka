#! /usr/bin/python3
import docker
#print(x)

def get_image_id(image_name):
    return [x[1] for x in ((x.attrs['RepoTags'][0], x.attrs['Id']) for x in client.images.list()) if x[0] == image_name][0]

client = docker.from_env()

image_id = get_image_id('apache/beam_java8_sdk:2.36.0')
containers = client.containers.list(all = True)
containers = [c for c in client.containers.list(all = True) if c.attrs['Image'] == image_id]

for c in containers:
    c.remove()

    
