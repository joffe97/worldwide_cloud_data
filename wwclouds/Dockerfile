FROM ubuntu:20.04

WORKDIR /usr/src/wwclouds

RUN apt-get update -y 
RUN apt-get install apt-utils -y 
RUN apt-get install dialog apt-utils -y 
RUN echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections 
RUN apt-get install -y -q 
RUN apt-get install curl -y

RUN apt-get install software-properties-common -y 
RUN add-apt-repository ppa:deadsnakes/ppa -y
RUN apt-get update 
RUN apt autoremove python3 -y
RUN apt-get install python3.9-distutils -y
RUN apt-get install python3-apt -y
RUN apt-get install python3.9 -y 

RUN curl -sSL https://bootstrap.pypa.io/pip/3.6/get-pip.py --output get-pip.py
RUN python3.9 get-pip.py
RUN python3.9 -m pip install --upgrade pip

RUN apt-get install software-properties-common -y 
RUN add-apt-repository ppa:ubuntugis/ppa 
RUN apt-get update 
RUN apt-get install gdal-bin=3.3.2+dfsg-2~focal2 -y 
RUN apt-get install libgdal-dev=3.3.2+dfsg-2~focal2 -y
RUN apt-get install ffmpeg libsm6 libxext6 -y

RUN export CPLUS_INCLUDE_PATH=/usr/include/gdal
RUN export C_INCLUDE_PATH=/usr/include/gdal

COPY requirements.txt ./
RUN python3.9 -m pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT python3.9 . imagevisual 20000 && tail -f /dev/null
