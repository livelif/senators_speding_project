FROM python:3.9.5-slim-buster

WORKDIR /usr/src/app

USER root

RUN mkdir -p /usr/share/man/man1

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk ca-certificates-java && \
    apt-get clean && \
    update-ca-certificates -f

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64/

RUN export JAVA_HOME

# Prints installed java version, just for checking
RUN java --version
#ENV JAVA_HOME='/usr/lib/jvm/java-8-openjdk-amd64'
ENV PATH=$PATH:$JAVA_HOME/bin
#RUN source ~/.bashrc
#ENV SPARK_HOME='/opt/spark/'
#ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

RUN echo $PATH
RUN java --version
#RUN ls /usr/lib/jvm/java-8-openjdk-amd64

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# install dependencies
RUN pip install --upgrade pip
COPY ./requirements.txt /usr/src/app/requirements.txt
RUN pip install -r requirements.txt

# copy project
COPY . /usr/src/app/

RUN pip install pyspark

ENV SPARK_WORKER_DIR /usr/src/app/services/web/project
