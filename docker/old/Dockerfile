FROM node:latest

RUN apt-get update && \
    apt-get install -y git vim.tiny curl jq && \
    curl -L -sk -o /usr/local/bin/jq "https://github.com/stedolan/jq/releases/download/jq-1.5/jq-linux64" && \
    chmod a+x /usr/local/bin/jq


    # install the cli from git
RUN git clone https://github.com/agaveplatform/agave-cli /usr/local/agave-cli && \
    chmod +x /usr/local/agave-cli/bin/*


RUN git clone https://github.com/jardmclan/rfdata_processor.git /usr/local/ingestor && \
    chmod +x /usr/local/ingestor/bin/agave_local/* && \
    chmod +x /usr/local/ingestor/bin/agave_containerized/*


    # set user's default env. This won't get sourced, but is helpful
RUN echo HOME=/root >> /root/.bashrc && \
    echo PATH=/usr/local/agave-cli/bin:$PATH >> /root/.bashrc && \
    echo export PS1=\""\[\e[32;4m\]agave-cli\[\e[0m\]:\u@\h:\w$ "\" >> /root/.bashrc

ENV ENV /root/.bashrc

ENV PATH /usr/local/agave-cli/bin:$PATH
ENV PS1 \""\[\e[32;4m\]agave-cli\[\e[0m\]:\u@\h:\w$ "\"

VOLUME /root/.agave

WORKDIR /usr/local/ingestor
RUN npm install


ENTRYPOINT ["node", "processor.js"]
