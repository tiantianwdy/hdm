FROM openjdk:8u131-jre
# Start building n1-distributed environment
RUN echo "Started building n1-distributed environment"

RUN [ "sh", "-c", "echo $JAVA_HOME"]

# Building HDM ENV

RUN mkdir -p /home/ubuntu/lib/hdm
ADD hdm-engine/target/hdm-engine.zip /home/ubuntu/lib/hdm/
RUN cd /home/ubuntu/lib/hdm/ &&\
    unzip hdm-engine.zip

RUN [ "sh", "-c", "ls /home/ubuntu/lib/hdm/| echo"]

RUN [ "sh", "-c", "chmod +x /home/ubuntu/lib/hdm/hdm-engine/*.sh"]

RUN [ "sh", "-c", "echo Building hdm env succeeded at:[lib/hdm/hdm-engine]"]

EXPOSE 80 8998 9001 9091 10010 10001 12010 

WORKDIR /home/ubuntu/lib/hdm/hdm-engine

ENTRYPOINT ["./hdm-deamon.sh"]
CMD ["start", "master", "-p", "8998"]
# CMD ["sh", "-c", "startup-master.sh -p 8998"]

