FROM golang:1.23

WORKDIR /app

ENV GOPROXY=off \
    GOSUMDB=off

COPY go.mod go.sum ./
COPY vendor/ ./vendor/
COPY . .
RUN go build -mod=vendor ./...

CMD ["bash"]
