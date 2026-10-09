FROM elixir:1.18-alpine AS build
RUN apk add --no-cache build-base git
WORKDIR /app
RUN mix local.hex --force && mix local.rebar --force
ENV MIX_ENV=prod
COPY mix.exs mix.lock ./
RUN mix deps.get --only prod
COPY config config
RUN mix deps.compile
COPY lib lib
RUN mix compile && mix release

FROM elixir:1.18-alpine
RUN apk add --no-cache libstdc++ openssl ncurses-libs ca-certificates curl && addgroup -g 10001 broker && adduser -D -u 10001 -G broker broker
WORKDIR /app
COPY --from=build --chown=broker:broker /app/_build/prod/rel/agora_broker ./
USER broker
ENV HOME=/app
EXPOSE 4443
CMD ["/app/bin/agora_broker", "start"]
