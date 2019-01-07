FROM registry.eng.hortonworks.com/hortonworks/hwx_nginx:1.13.3
MAINTAINER Hortonworks

ADD nginx/nginx.conf /etc/nginx/nginx.conf
RUN rm -rf /etc/nginx/conf.d/*.conf
ADD nginx/conf.d/default.conf /etc/nginx/conf.d/default.conf

COPY static /usr/das-app/

ADD https://releases.hashicorp.com/consul/0.8.5/consul_0.8.5_linux_amd64.zip /

RUN apt-get install -y unzip && \
    unzip -d /bin consul_0.8.5_linux_amd64.zip && \
    rm consul_0.8.5_linux_amd64.zip && \
    apt-get remove --purge --yes unzip

COPY consul.config /scripts/
COPY start.sh /scripts/

EXPOSE 80 443
ENTRYPOINT ["bash", "/scripts/start.sh"]
