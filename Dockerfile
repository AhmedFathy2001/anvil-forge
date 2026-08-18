# Anvil.Forge — the data plane. Single static binary, no runtime dependencies.
#
# Built by CI and pushed to GHCR; the box never builds. Mirrors Anvil.Site's arrangement so the
# deploy story is the same one you already operate.

# ── Build ────────────────────────────────────────────────────────────────────────────────────────
FROM golang:1.23-alpine AS builder

WORKDIR /src

# Dependencies first, as their own layer: go.mod/go.sum change far less often than the code, so an
# ordinary commit reuses the cached module download instead of re-fetching every dependency.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG GIT_SHA=unknown

# CGO off so the result is genuinely static and can run on `scratch`. Trimpath keeps build-machine
# paths out of panics; -w -s drops DWARF and the symbol table, which is most of the binary size and
# none of the usefulness in production (stack traces still carry function names).
RUN CGO_ENABLED=0 GOOS=linux go build \
      -trimpath \
      -ldflags="-w -s -X main.gitSHA=${GIT_SHA}" \
      -o /out/forge ./cmd/forge

# ── Run ──────────────────────────────────────────────────────────────────────────────────────────
# Static base: no shell, no package manager, nothing to exploit. It carries CA certificates (needed
# for TLS to the hiscores and Discord) and /etc/passwd for the nonroot user, which `scratch` does not.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/forge /forge

# Forge opens no files and needs no writable filesystem — everything is Postgres and outbound HTTPS.
USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/forge"]
