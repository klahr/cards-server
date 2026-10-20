FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /cards-server .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /cards-server /cards-server
EXPOSE 7777
ENTRYPOINT ["/cards-server"]
