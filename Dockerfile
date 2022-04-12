# Use an official Python runtime as a base image
FROM python:3.7-slim

## The maintainer name and email
LABEL maintainer="CHIME/FRB Collaboration"

#RUN set -xe 

RUN apt-get update && \
    #--------------------------------------------
    #Install any needed packages to install comet
    #--------------------------------------------
    apt-get install -y software-properties-common && \ 
    apt-get install -y apt-utils && \
    apt-get install -y git && \
    apt-get install -y curl && \
    apt-get install -y build-essential && \
    apt-get install -y libmariadb-dev && \
    git clone --branch vs/comet https://github.com/aelanman/comet.git && \
    pip install -r /comet/requirements.txt && \
    pip install /comet && \
    #-----------------------
    # Minimize container size
    #-----------------------
    apt-get remove -y curl git && \
    apt-get autoremove -y && \
    apt-get clean -y && \
    rm -rf /tmp/build 

# Run comet when the container launches
CMD comet --debug 1 --recover 0
