FROM golang:1.5

COPY Godeps/_workspace /go
COPY internal /go/src/fknsrs.biz/p/jobserver/internal
COPY cmd /go/src/fknsrs.biz/p/jobserver/cmd
COPY *.go /go/src/fknsrs.biz/p/jobserver/

RUN go install fknsrs.biz/p/jobserver/cmd/jobserverd

EXPOSE 2097
VOLUME /data
ENV DB_PATH=/data/jobs.db

CMD jobserverd
