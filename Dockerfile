FROM ubuntu:latest
LABEL authors="alonm"

ENTRYPOINT ["top", "-b"]