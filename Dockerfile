FROM sofraserv/financedb_base:test

WORKDIR /var/www/HistFlaskDocker
RUN    mkdir /var/www/HistFlaskDocker/logs

RUN    apt-get update

RUN    echo y | apt-get install unixodbc unixodbc-dev
RUN    echo y | apt-get install locales
RUN    echo y | apt-get install ufw
#RUN   echo y | apt-get install selinux-basics selinux-policy-default auditd
RUN    echo y | apt-get install libpam-pwdfile
RUN    sed -i 's/^# *\(en_US.UTF-8\)/\1/' /etc/locale.gen
RUN    locale-gen en_US.UTF-8  
ENV    LANG en_US.UTF-8  
ENV    LANGUAGE en_US:en  
ENV    LC_ALL en_US.UTF-8

COPY   requirements.txt requirements.txt
RUN    pip3 install -r requirements.txt

COPY   . /var/www/HistFlaskDocker
#RUN    chown -R powerauto:powerauto /home/powerauto/data

ADD start.sh /var/www/HistFlaskDocker/start.sh
RUN chmod +x /var/www/HistFlaskDocker/start.sh
CMD ["/var/www/HistFlaskDocker/start.sh"]