############################################################
# Dockerfile to build MongoDB container images
# Based on Ubuntu
############################################################
# Set the base image to Ubuntu
FROM ubuntu
RUN apt-get update
# Common
RUN echo "Installing common packages"
RUN apt-get install -y vim
RUN apt-get install -y curl
RUN apt-get install -y python-software-properties
RUN apt-get install -y software-properties-common
RUN apt-get install -y python-pip
RUN apt-get install -y wget
RUN apt-get update

# Scala
RUN echo "Installing Scala 2.10...."
RUN wget http://www.scala-lang.org/files/archive/scala-2.10.6.tgz
RUN mkdir /usr/local/src/scala
RUN tar xvf scala-2.10.6.tgz -C /usr/local/src/scala/
RUN echo "Exporting Scala paths..."
ENV SCALA_HOME="/usr/local/src/scala/scala-2.10.6"
ENV PATH="$SCALA_HOME/bin:${PATH}"


# Java
RUN echo "Installing Java 7.."
#RUN add-apt-repository ppa:openjdk-r/ppa
RUN add-apt-repository -y ppa:webupd8team/java
RUN apt-get update
RUN echo oracle-java7-installer shared/accepted-oracle-license-v1-1 select true | /usr/bin/debconf-set-selections
RUN apt-get install -y oracle-java7-installer


# Maven
RUN echo "Installing Maven3...."
RUN apt-get install -y maven
RUN echo "Setting maven OPTS.."
ENV MAVEN_OPTS="-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m"

# AWS Cli
RUN echo "Installing AWSCLI..."
RUN pip install awscli
# Git
RUN echo "Installing git...."
RUN apt-get install -y git
# Spark
RUN echo "Installing Apache spark 2.0"
RUN git clone git://github.com/apache/spark.git
WORKDIR /spark
RUN ./dev/change-scala-version.sh 2.10

#RUN mvn -Pyarn -Dscala-2.10 -DskipTests clean package
RUN ./build/mvn -Pyarn -Dscala-2.10 -DskipTests clean compile
RUN echo "Now packaging......"
RUN ./build/mvn -Pyarn -Dscala-2.10 -DskipTests package
ENV SPARK_HOME="/spark"
ENV PATH="$SPARK_HOME/bin:${PATH}"
