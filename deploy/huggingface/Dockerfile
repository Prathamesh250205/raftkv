# Hugging Face Space for the raftkv live demo. The Space needs only this file
# and README.md; the code is built from the GitHub repository. To pick up new
# commits, use "Factory rebuild" in the Space settings.
FROM golang:1.22-alpine AS build
RUN apk add --no-cache git
ARG RAFTKV_REF=main
RUN git clone --depth 1 --branch ${RAFTKV_REF} https://github.com/Prathamesh250205/raftkv /src
WORKDIR /src
RUN CGO_ENABLED=0 go build -trimpath -o /out/kvdemo ./cmd/kvdemo

FROM alpine:3.20
# Spaces run containers as user ID 1000.
RUN adduser -D -u 1000 user
USER user
COPY --from=build /out/kvdemo /usr/local/bin/kvdemo
ENV PORT=7860
EXPOSE 7860
CMD ["kvdemo"]
