LogIsland docker files
======================

Small standalone Hadoop distribution for development and testing purpose :

- Spark 1.6.2
- Elasticsearch 2.3.3
- Kibana 4.5.1
- Kafka 0.9.0.1
- Logisland 1.1.0


This repository contains a Docker file to build a Docker image with Apache Spark, HBase, Flume & Zeppelin. 
This Docker image depends on [centos 6.7](https://github.com/CentOS/CentOS-Dockerfiles) image.

Getting the docker image from repository
----------------------------------------

Pull the image from Docker Repository

.. code-block:: sh

    docker pull hurence/logisland


Build your own
--------------

Building the image

.. code-block:: sh

    # build logisland
    mvn clean package
    cp logisland-assembly/target/logisland-1.1.0-bin.tar.gz logisland-docker

The archive is generated under dist directory, 
you have to copy this file into your Dockerfile directory you can now issue

.. code-block:: sh

    docker build --rm -t hurence/logisland-job -f Dockerfile .
    docker tag hurence/logisland-job:latest hurence/logisland-job:1.1.0


Running the image
-----------------

* if using boot2docker make sure your VM has more than 2GB memory
* in your /etc/hosts file add $(boot2docker ip) as host 'sandbox' to make it easier to access your sandbox UI
* open yarn UI ports when running container

.. code-block:: sh

    docker run -it --name logisland-job hurence/logisland-job

or

.. code-block::

    docker run -d -h sandbox hurence/logisland:1.1.0 -d

if you want to mount a directory from your host, add the following option :

.. code-block::

    -v ~/projects/logisland/docker/mount/:/usr/local/logisland


Deploy the image to Docker hub
------------------------------

tag the image as latest

.. code-block:: sh

    # verify image build
    docker images
    docker tag <IMAGE_ID> latest


then login and push the latest image

.. code-block:: sh

    docker login
    docker push hurence/logisland
