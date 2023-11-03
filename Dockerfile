FROM sofraserv/financedb_base:test

WORKDIR /var/www/HistFlaskDocker
RUN    mkdir /var/www/HistFlaskDocker/logs

COPY   requirements.txt requirements.txt
RUN    pip3 install -r requirements.txt

COPY   . /var/www/HistFlaskDocker
#RUN    chown -R powerauto:powerauto /home/powerauto/data

ADD start.sh /var/www/HistFlaskDocker/start.sh
RUN chmod +x /var/www/HistFlaskDocker/start.sh
CMD ["/var/www/HistFlaskDocker/start.sh"]