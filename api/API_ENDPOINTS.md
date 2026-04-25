# API Endpoints Map (ASCII)

```text
API Root
|-- /docs [GET] (app.docs_redirect) - Redirects to active Swagger UI path :: api/service/app.py
|-- /openapi.json [GET] (app.openapi_redirect) - Redirects to active OpenAPI JSON path :: api/service/app.py
|-- /healthz [GET] (app.healthz) - K8s Liveness Probe (Returns 200 OK) :: api/service/app.py
|-- /readyz [GET] (app.readyz) - K8s Readiness Probe (Shallow 200/ready response; no dependency check) :: api/service/app.py
|-- /config.js [GET] (app.serve_runtime_config) - Serves runtime env vars to UI :: api/service/app.py <== ui/src/config.ts (implicit)
|-- /api/ws/updates [WEBSOCKET] (realtime.websocket_updates) - Real-time updates for UI :: api/endpoints/realtime.py
`-- /api
    |-- /docs [GET] (app.swagger_ui) - Browser Swagger UI docs :: api/service/app.py
    |-- /openapi.json [GET] (app.openapi_json) - OpenAPI spec payload :: api/service/app.py
    |-- /auth/session [GET] (auth.get_auth_session) - Returns the authenticated session summary used by the UI auth boundary :: api/endpoints/auth.py
    |-- /ai
    |   `-- /chat/stream [POST] (ai.stream_chat) - Authenticated SSE relay to the OpenAI Responses API; accepts JSON or multipart with optional files :: api/endpoints/ai.py
    |-- /realtime/ticket [POST] (realtime.issue_realtime_ticket) - Issues a single-use websocket ticket for authenticated clients :: api/endpoints/realtime.py <== ui/src/hooks/useRealtime.ts
    |-- /ws/updates [WebSocket] (realtime.websocket_updates) - Real-time updates (system health/jobs/container apps/runtime config/debug symbols) :: api/endpoints/realtime.py <== ui/src/hooks/useRealtime.ts

    # System & Health (Matches ui/src/hooks/useDataQueries.ts)
    |-- /system
    |   |-- /health [GET] (system.system_health) - Returns overall system status, layer freshness, and active alerts :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /lineage [GET] (system.system_lineage) - Returns data lineage graph and dependencies :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /discovery/catalog [GET] (system.get_discovery_catalog) - Supported dataset catalog for control-plane and canonical Gold discovery :: api/endpoints/system.py
    |   |-- /discovery/datasets/{schema_name}/{table_name} [GET] (system.get_discovery_dataset_detail) - Field-level metadata, types, descriptions, and default sort for one dataset :: api/endpoints/system.py
    |   |-- /discovery/datasets/{schema_name}/{table_name}/sample [GET] (system.get_discovery_dataset_sample) - Read-only bounded sample preview for one dataset :: api/endpoints/system.py
    |   |-- /debug-symbols [GET] (system.get_debug_symbols) - Returns runtime-config-backed debug-symbol state :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /debug-symbols [POST] (system.set_debug_symbols) - Updates runtime-config-backed debug-symbol state :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /runtime-config/catalog [GET] (system.get_runtime_config_catalog) - Lists allowlisted runtime-config keys :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /runtime-config [GET] (system.get_runtime_config) - Lists runtime-config overrides for a scope :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /runtime-config [POST] (system.set_runtime_config) - Upserts a runtime-config override :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /runtime-config/{key} [DELETE] (system.remove_runtime_config) - Deletes a runtime-config override :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /purge-candidates [GET] (system.get_purge_candidates) - Synchronous purge-candidate preview (manual) :: api/endpoints/system.py
    |   |-- /purge-candidates [POST] (system.create_purge_candidates_operation) - Queues async purge-candidate preview operation (202 + operationId) :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /purge [POST] (system.purge_data) - Queues layer/domain purge operation :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /purge/{operation_id} [GET] (system.get_purge_operation) - Polls purge/preview operation status/result :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   |-- /purge-symbols [POST] (system.purge_symbols) - Queues selected-symbol purge batch :: api/endpoints/system.py <== ui/src/services/DataService.ts
    |   `-- /jobs
    |       |-- /{job_name}/run [POST] (system.trigger_job_run) - Manually triggers an Azure Container App Job :: api/endpoints/system.py <== ui/src/hooks/useJobTrigger.ts
    |       |-- /{job_name}/suspend [POST] (system.suspend_job) - Suspends an Azure Container App Job :: api/endpoints/system.py <== ui/src/hooks/useJobSuspend.ts
    |       |-- /{job_name}/resume [POST] (system.resume_job) - Resumes an Azure Container App Job :: api/endpoints/system.py <== ui/src/hooks/useJobSuspend.ts
    |       `-- /{job_name}/logs [GET] (system.get_job_logs) - Returns log tail for last N Job runs :: api/endpoints/system.py <== ui/src/app/components/pages/system-status/JobLogDrawer.tsx

    # Backtest Data & Execution (Matches ui/src/services/backtestHooks.ts)
    |-- /backtests [GET] (backtests.list_backtests) - Lists historical backtest runs with filtering :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
    |-- /backtests [POST] (backtests.submit_backtest) - Submits a new backtest job :: api/endpoints/backtests.py <== ui/src/app/components/pages/StrategyConfigPage.tsx
    `-- /backtests/{run_id}
        |-- /status [GET] (backtests.get_status) - Polls current status of a running backtest :: api/endpoints/backtests.py
        |-- /summary [GET] (backtests.get_summary) - Returns performance summary plus additive v4 metadata, cost-drag fields, and closed-position statistics from Postgres :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        |-- /trades [GET] (backtests.get_trades) - Returns executed trade audit rows, including `position_id` and `trade_role`, for a run from Postgres :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        |-- /positions/closed [GET] (backtests.get_closed_positions) - Returns flat-to-flat closed position cycles with realized PnL, return, costs, and exit reason :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        |-- /metrics
        |   |-- /timeseries [GET] (backtests.get_timeseries) - Returns equity curve and drawdown series with additive period_return metadata from Postgres :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        |   `-- /rolling [GET] (backtests.get_rolling_metrics) - Returns rolling metrics with additive window_periods metadata from Postgres :: api/endpoints/backtests.py <== ui/src/services/backtestHooks.ts
        `-- Results are Postgres-backed only; no backtest artifact endpoints remain.

    # Providers (ETL Gateway)
    |-- /providers
    |   |-- /alpha-vantage
    |   |   |-- /listing-status [GET] (alpha_vantage.get_listing_status) - Alpha Vantage LISTING_STATUS CSV :: api/endpoints/alpha_vantage.py
    |   |   |-- /time-series/daily [GET] (alpha_vantage.get_daily_time_series) - Alpha Vantage TIME_SERIES_DAILY CSV :: api/endpoints/alpha_vantage.py
    |   |   |-- /earnings [GET] (alpha_vantage.get_earnings) - Alpha Vantage EARNINGS payload :: api/endpoints/alpha_vantage.py
    |   |   `-- /earnings-calendar [GET] (alpha_vantage.get_earnings_calendar) - Alpha Vantage earnings calendar CSV :: api/endpoints/alpha_vantage.py
    |   |-- /kalshi
    |   |   |-- /markets [GET] (kalshi.kalshi_markets) - Returns Kalshi market metadata with filtering and pagination :: api/endpoints/kalshi.py
    |   |   |-- /markets/{ticker} [GET] (kalshi.kalshi_market) - Returns one Kalshi market :: api/endpoints/kalshi.py
    |   |   |-- /markets/{ticker}/orderbook [GET] (kalshi.kalshi_orderbook) - Returns the Kalshi yes/no bid book for one market :: api/endpoints/kalshi.py
    |   |   |-- /balance [GET] (kalshi.kalshi_balance) - Returns Kalshi available balance and portfolio value :: api/endpoints/kalshi.py
    |   |   |-- /positions [GET] (kalshi.kalshi_positions) - Returns Kalshi market and event positions :: api/endpoints/kalshi.py
    |   |   |-- /orders [GET] (kalshi.kalshi_orders) - Returns Kalshi orders with provider filters :: api/endpoints/kalshi.py
    |   |   |-- /orders [POST] (kalshi.kalshi_create_order) - Creates a Kalshi order using explicit side/action semantics :: api/endpoints/kalshi.py
    |   |   |-- /orders/{order_id} [GET] (kalshi.kalshi_order) - Returns one Kalshi order :: api/endpoints/kalshi.py
    |   |   |-- /orders/{order_id} [DELETE] (kalshi.kalshi_cancel_order) - Cancels a Kalshi order and returns the reduced quantity :: api/endpoints/kalshi.py
    |   |   |-- /orders/{order_id}/amend [POST] (kalshi.kalshi_amend_order) - Amends a Kalshi order price and/or quantity :: api/endpoints/kalshi.py
    |   |   |-- /orders/{order_id}/queue-position [GET] (kalshi.kalshi_order_queue_position) - Returns the queue position for one resting Kalshi order :: api/endpoints/kalshi.py
    |   |   |-- /orders/queue-positions [GET] (kalshi.kalshi_queue_positions) - Returns queue positions for matching Kalshi resting orders :: api/endpoints/kalshi.py
    |   |   `-- /account/limits [GET] (kalshi.kalshi_account_limits) - Returns Kalshi API tier and read/write limits :: api/endpoints/kalshi.py
    |   |-- /etrade
    |   |   |-- /connect/start [POST] (etrade.etrade_connect_start) - Starts E*TRADE OAuth and returns the authorize URL plus callback metadata when configured :: api/endpoints/etrade.py
    |   |   |-- /connect/complete [POST] (etrade.etrade_connect_complete) - Completes manual E*TRADE OAuth verifier exchange :: api/endpoints/etrade.py
    |   |   |-- /connect/callback [GET] (etrade.etrade_connect_callback) - Unauthenticated browser callback for E*TRADE OAuth completion :: api/endpoints/etrade.py
    |   |   |-- /connect/callback-url [GET] (etrade.etrade_connect_callback_url) - Authenticated discovery route that returns the canonical E*TRADE callback URL to register with the provider :: api/endpoints/etrade.py
    |   |   |-- /session [GET] (etrade.etrade_session) - Returns current E*TRADE connection state :: api/endpoints/etrade.py
    |   |   |-- /disconnect [POST] (etrade.etrade_disconnect) - Revokes the local E*TRADE session :: api/endpoints/etrade.py
    |   |   |-- /accounts [GET] (etrade.etrade_accounts) - Lists available E*TRADE accounts :: api/endpoints/etrade.py
    |   |   |-- /accounts/{account_key}/balance [GET] (etrade.etrade_balance) - Returns account balances :: api/endpoints/etrade.py
    |   |   |-- /accounts/{account_key}/portfolio [GET] (etrade.etrade_portfolio) - Returns account positions :: api/endpoints/etrade.py
    |   |   |-- /accounts/{account_key}/transactions [GET] (etrade.etrade_transactions) - Returns account transactions :: api/endpoints/etrade.py
    |   |   |-- /accounts/{account_key}/transactions/{transaction_id} [GET] (etrade.etrade_transaction_details) - Returns transaction details :: api/endpoints/etrade.py
    |   |   |-- /quotes [GET] (etrade.etrade_quotes) - Returns E*TRADE quotes :: api/endpoints/etrade.py
    |   |   |-- /orders [GET] (etrade.etrade_orders) - Returns E*TRADE orders :: api/endpoints/etrade.py
    |   |   |-- /orders/preview [POST] (etrade.etrade_preview_order) - Previews an E*TRADE order :: api/endpoints/etrade.py
    |   |   |-- /orders/place [POST] (etrade.etrade_place_order) - Places an E*TRADE order :: api/endpoints/etrade.py
    |   |   `-- /orders/cancel [POST] (etrade.etrade_cancel_order) - Cancels an E*TRADE order :: api/endpoints/etrade.py
    |   |-- /massive
    |   |   |-- /time-series/daily [GET] (massive.get_daily_time_series) - Massive OHLCV CSV normalized to Date,Open,High,Low,Close,Volume :: api/endpoints/massive.py
    |   |   |-- /fundamentals/short-interest [GET] (massive.get_short_interest) - Massive short interest payload :: api/endpoints/massive.py
    |   |   |-- /fundamentals/short-volume [GET] (massive.get_short_volume) - Massive short volume payload :: api/endpoints/massive.py
    |   |   |-- /fundamentals/float [GET] (massive.get_float) - Massive float payload :: api/endpoints/massive.py
    |   |   `-- /financials/{report} [GET] (massive.get_finance_report) - Massive financial payload :: api/endpoints/massive.py
    |   `-- /schwab
    |       |-- /connect/start [POST] (schwab.schwab_connect_start) - Starts Schwab OAuth and returns the authorize URL plus state metadata :: api/endpoints/schwab.py
    |       |-- /connect/complete [POST] (schwab.schwab_connect_complete) - Completes manual Schwab OAuth code exchange with state validation :: api/endpoints/schwab.py
    |       |-- /connect/callback [GET] (schwab.schwab_connect_callback) - Unauthenticated browser callback for Schwab OAuth completion with pending-state validation :: api/endpoints/schwab.py
    |       |-- /connect/callback-url [GET] (schwab.schwab_connect_callback_url) - Authenticated discovery route that returns the canonical Schwab callback URL to register with the provider :: api/endpoints/schwab.py
    |       |-- /session [GET] (schwab.schwab_session) - Returns current in-memory Schwab broker session state :: api/endpoints/schwab.py
    |       |-- /disconnect [POST] (schwab.schwab_disconnect) - Clears the local Schwab broker session :: api/endpoints/schwab.py
    |       |-- /account-numbers [GET] (schwab.schwab_account_numbers) - Returns Schwab account number and hash metadata :: api/endpoints/schwab.py
    |       |-- /accounts [GET] (schwab.schwab_accounts) - Returns Schwab accounts with optional provider fields :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number} [GET] (schwab.schwab_account) - Returns one Schwab account with optional provider fields :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/balance [GET] (schwab.schwab_balance) - Returns Schwab account balances and provider account metadata :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/positions [GET] (schwab.schwab_positions) - Returns Schwab account positions and metadata :: api/endpoints/schwab.py
    |       |-- /orders [GET] (schwab.schwab_all_orders) - Returns Schwab orders across accounts :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/orders [GET] (schwab.schwab_account_orders) - Returns Schwab orders for one account :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/orders/preview [POST] (schwab.schwab_preview_order) - Previews a Schwab order :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/orders [POST] (schwab.schwab_place_order) - Places a Schwab order :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/orders/{order_id} [GET] (schwab.schwab_order) - Returns a Schwab order by id :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/orders/{order_id} [PUT] (schwab.schwab_replace_order) - Replaces a Schwab order :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/orders/{order_id} [DELETE] (schwab.schwab_cancel_order) - Cancels a Schwab order :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/transactions [GET] (schwab.schwab_transactions) - Returns Schwab account transaction history :: api/endpoints/schwab.py
    |       |-- /accounts/{account_number}/transactions/{transaction_id} [GET] (schwab.schwab_transaction) - Returns Schwab transaction details :: api/endpoints/schwab.py
    |       `-- /user-preference [GET] (schwab.schwab_user_preference) - Returns Schwab user preference metadata :: api/endpoints/schwab.py

    # Raw Data Layer
    |-- /data
    |   |-- /symbols [GET] (data.list_symbols) - Returns Postgres symbol universe :: api/endpoints/data.py <== ui/src/app/components/pages/StockExplorerPage.tsx
    |   |-- /screener [GET] (data.get_stock_screener) - Daily screener snapshot (Silver+Gold+Postgres) :: api/endpoints/data.py <== ui/src/app/components/pages/StockExplorerPage.tsx
    |   `-- /{layer}
    |       |-- /{domain} [GET] (data.get_data_generic) - generic accessor for Silver/Gold delta tables (prices, earnings) :: api/endpoints/data.py <== ui/src/services/DataService.ts
    |       `-- /finance/{sub_domain} [GET] (data.get_finance_data) - Specialized accessor for financial statements :: api/endpoints/data.py <== ui/src/services/DataService.ts
```
