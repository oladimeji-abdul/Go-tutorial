FROM golang:1.21-alpine AS build

WORKDIR /build
COPY . .
RUN apk add --no-cache git
RUN go env -w GOPROXY=direct
RUN go mod download
RUN go build -o ./userapiapp

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=build /build/userapiapp ./userapiapp
CMD ["/app/userapiapp"]
