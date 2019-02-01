FROM oraclelinux:7-slim

MAINTAINER oracle

# Install from yum
RUN echo "Installing EPEL, python-pip, unzip, libaio, oci_cli, requests, cx_Oracle"  && \
    yum -y install https://dl.fedoraproject.org/pub/epel/epel-release-latest-7.noarch.rpm && \
    yum -y install python && \
    yum -y install python-pip &&\
    yum -y install unzip && \
    yum -y install libaio && \ 
    yum -y install nodejs npm --enablerepo=epel && \
    yum -y install git && \
    yum -y install nano && \
    yum clean all && \
    echo 'installing oci_cli, requests, cx_Oracle' && \
    pip install oci_cli requests cx_Oracle

# install from pip
RUN echo 'installing oci_cli, requests, cx_Oracle' && \
    pip install oci_cli requests cx_Oracle tweepy simplejson textblob nltk && \
    python -m nltk.downloader stopwords

# Setup oracle instant client and sqlcl
ENV SQLPLUS oracle-instantclient12.2-sqlplus-12.2.0.1.0-1.x86_64.rpm
#ENV SQLCL sqlcl-18*.zip
ENV INSTANT_CLIENT oracle-instantclient12.2-basic-12.2.0.1.0-1.x86_64.rpm

WORKDIR /opt/oracle/lib
ADD ${INSTANT_CLIENT} ${SQLPLUS} ./
RUN echo "Installing instant client........" && \
   rpm -ivh ${INSTANT_CLIENT} && \
   echo "Installing SQL*Plus..........." && \
   rpm -ivh ${SQLPLUS} && \
   #unzip ${SQLCL} && \
   rm ${INSTANT_CLIENT} ${SQLPLUS} && \
   mkdir -p /opt/oracle/database/wallet && \
   mkdir -p /opt/oracle/tools/oci

#set env variables
ENV ORACLE_BASE /opt/oracle/lib/instantclient_12_2
ENV LD_LIBRARY_PATH /usr/lib/oracle/12.2/client64/lib/:$LD_LIBRARY_PATH
ENV TNS_ADMIN /opt/oracle/database/wallet/
ENV ORACLE_HOME /opt/oracle/lib/instantclient_12_2
ENV PATH $PATH:/usr/lib/oracle/12.2/client64/bin:/opt/oracle/lib/sqlcl/bin

# get python application from git repo
RUN git clone https://github.com/Abdul-Rafae-Mohammed/Real-Time_Twitter-Data_Analysis.git
RUN mkdir wallet_NODEAPPDB2
COPY ./wallet_NODEAPPDB2 ./wallet_NODEAPPDB2
