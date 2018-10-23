FROM registry.eng.hortonworks.com/hortonworks/hwx_nginx:1.13.3
MAINTAINER Hortonworks

ADD nginx/nginx.conf /etc/nginx/nginx.conf
RUN rm -rf /etc/nginx/conf.d/*.conf
ADD nginx/conf.d/default.conf /etc/nginx/conf.d/default.conf

COPY static /usr/das-app/

RUN apt-get update && \
    apt-get install -y gnupg wget procps unzip && \
    apt-key adv --keyserver keyserver.ubuntu.com --recv-keys B9733A7A07513CAD && \
    wget https://releases.hashicorp.com/consul/0.8.5/consul_0.8.5_linux_amd64.zip && \
    unzip -d /bin consul_0.8.5_linux_amd64.zip && \
    rm consul_0.8.5_linux_amd64.zip && \
    apt-get remove --purge --yes gnupg wget unzip && \
    apt-get clean all

# RUN rm /etc/nginx/sites-enabled/default

COPY consul.config /scripts/
COPY start.sh /scripts/

EXPOSE 80 443
ENTRYPOINT ["bash", "/scripts/start.sh"]
