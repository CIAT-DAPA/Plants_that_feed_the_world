# ITPGRFA Crop indicator docker
This repository contains all resources needed to execute the analysis of importance
for the conservation of genetics resources

## How to use
The following section explains a couple of things that you should take into account when you
are trying to run the image.

### Install
The installation process requires that **Docker** system is installed. Once you have installed
the Docker you just need to execute the following command, which download the image form Docker hub:

``` bash
docker pull stevensotelo/itpgrfa_indicator:latest
```

### Run
This image has a folder in which you can share information between host and the docker container. The folder
is **workdir** (located in /indicator). You should connect a folder from your PC with this volume in the
image. The following command describes how to execute an instance of the images like a Docker container, further
it connects a local folder (D:/my_folder/workdir) with the volume in the docker (/indicator).

``` bash
docker run -it --rm -v D:/my_folder/workdir/:/indicator/ stevensotelo/itpgrfa_indicator:latest /bin/bash
```

# VERSIONS
