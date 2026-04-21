# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Required toolchain

- Go `1.25.8` (from `go.mod`)
- Node `24` (CI uses `24.11.1`; `.nvmrc` currently contains `20`)
- Yarn `4.x` (`packageManager: yarn@4.13.0`)

## Common commands

### Setup

- Install frontend deps: `yarn install --frozen-lockfile`
- Install backend deps: `go mod download`

### Frontend (TypeScript/Grafana plugin UI)

- Dev watch build: `yarn dev`
- Production build: `yarn build`
- Lint: `yarn lint`
- Typecheck: `yarn typecheck`
- Unit tests: `yarn test`
- Single test file: `yarn test -- src/datasource.test.ts`
- Single test by name: `yarn test -- -t "test name"`

### Backend (Go plugin binary)

- Build backend binaries (mage): `make vl-backend-plugin-build`
- Go tests: `make golang-test`
- Go race tests: `make golang-test-race`
- Go lint/format/vet bundle used in CI: `make check-all`
- Single Go package test: `go test ./pkg/plugin`
- Single Go test by name: `go test ./pkg/plugin -run TestName`

### Full plugin packaging/validation

- Build frontend+backend: `make vl-plugin-build`
- Build and validate distributable plugin zip/tar: `make vl-plugin-check`

### Local Grafana/VictoriaLogs run

- Build plugin first: `make vl-plugin-build`
- Start local stack: `docker compose up`

`compose.yaml` mounts `./plugins/victoriametrics-logs-datasource` into Grafana, so building before compose is required.

## Architecture overview

### Runtime shape

This is a **Grafana datasource plugin with both frontend and backend components**:

- Frontend plugin registration: `src/module.ts`
- Plugin metadata/capabilities: `src/plugin.json`
- Backend executable entrypoint: `pkg/main.go`

`src/plugin.json` has `backend: true` and executable `victoriametrics_logs_backend_plugin`, so Grafana routes queries through the Go backend.

### Frontend query pipeline

Core logic lives in `src/datasource.ts` (`VictoriaLogsDatasource`):

- Extends `DataSourceWithBackend` and sends query requests to backend.
- Normalizes query targets (format, maxLines, step, timezone offset, extra filters).
- Supports live mode via `runLiveQueryThroughBackend`.
- Applies post-processing with `transformBackendResult` (`src/transformers/transformBackendResult.ts`) to split/process stream vs metric/histogram frames and improve backend errors.

Editor selection is app-aware:

- `src/components/QueryEditor/QueryEditorByApp.tsx` switches to alerting-specific editor for `CoreApp.CloudAlerting`.

Autocomplete and field discovery are frontend-driven through `src/language_provider.ts`, which calls backend resource endpoints via `postResource(...)` and caches field/stream metadata results.

### Backend query/resource pipeline

Backend datasource implementation is centered in `pkg/plugin/datasource.go`:

- Implements `QueryData`, `CheckHealth`, streaming (`SubscribeStream`/`RunStream`), and resource handlers.
- Creates per-instance HTTP clients from Grafana datasource settings (including forwarded/custom headers).
- Handles resource proxy endpoints for field/stream metadata and tenant IDs:
  - `/select/logsql/field_values`
  - `/select/logsql/field_names`
  - `/select/logsql/streams`
  - `/select/logsql/stream_field_names`
  - `/select/logsql/stream_field_values`
  - `/select/tenant_ids`
  - `/vmui`

Query URL construction is in `pkg/plugin/query.go`:

- Maps query types to VictoriaLogs endpoints:
  - instant: `/select/logsql/query`
  - stats: `/select/logsql/stats_query`
  - stats range: `/select/logsql/stats_query_range`
  - hits: `/select/logsql/hits`
  - tail/live: `/select/logsql/tail`
- Applies `extra_filters`, `extra_stream_filters`, computed time range, and step logic.

Response decoding is in `pkg/plugin/response.go`:

- Instant/tail responses are line-decoded into Grafana log frames (`Time`, `Line`, `labels`).
- Stats/hits responses become metric-style frames.
- Streaming sends frames incrementally through Grafana Live.

Resource query payload parsing (field queries) is in `pkg/plugin/fields_query.go`.

### Build/output coupling details

- Backend build output path is set in `Magefile.go` to `plugins/victoriametrics-logs-datasource`.
- Frontend webpack config (`webpack.config.ts`) preserves backend binaries/manifests during frontend clean step via output `clean.keep` regex.
- This coupling is important: frontend builds should not remove backend artifacts in the plugin folder.
