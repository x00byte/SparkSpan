# SparkSpan

![SparkSpan: independent model servers behind one API](assets/hero.png)

SparkSpan routes OpenAI-compatible chat and completion requests across independent vLLM servers through one endpoint. Run the same model on several DGX Sparks over a normal LAN, or expose several different models on one machine or several machines.

Each request runs on one backend. Replicas own their weights and KV caches; the gateway balances requests rather than splitting a model across GPUs. A dedicated GPU interconnect is unnecessary for this arrangement. Every model must fit on its assigned backend, and the machines still need ordinary network connectivity.

## Inspiration

1. **Keep production running while testing.** Leave production on Spark A, drain Spark B, then use B to test models or build recipes. Restore its production model and resume routing when ready, without restarting A. Capacity is temporarily lower, so queues and latency can increase. [Maintenance workflow](docs/operations.md#testing-on-one-spark).

2. **Use several models through one endpoint.** Run different model servers across machines or several on one box, without constantly changing client ports, endpoints or connection settings. Each request's `model` selects a matching backend, and shared host admission limits control their combined request load. [Multi-model setup](#several-models-one-endpoint).

## What it does

- Discovers served model names and merges them into `GET /v1/models`. Routes each request only to a backend advertising the requested model.
- Balances replicas using available slots and estimated prefill work. Soft conversation affinity encourages prefix-cache reuse without preventing overflow to another replica.
- Enforces per-backend admission limits and optional shared host limits when several model servers share a machine.
- Passes through streaming SSE, non-streaming results, tool calls, reasoning and usage. Client cancellation closes the upstream connection and releases its slot.
- Drains one backend for maintenance without cancelling its active responses or restarting the other model.
- Provides bounded queues, connection-failure failover, backend health checks and aggregate Prometheus metrics with a `replica` label.
- Includes an optional custom dashboard for live metrics, rolling windows, deliberate benchmarks and admission controls.

SparkSpan runs on the CPU and does not load weights, quantize models, share KV caches, or change inference kernels. It does not automatically load or evict models. Shared host limits count requests; they cannot guarantee that several loaded models fit in GPU memory.

## Quick start

Start your model servers first and check that each exposes `/health`, `/metrics` and `/v1/models`. The gateway expects vLLM-style endpoints and metrics. Other OpenAI-compatible engines need their own validation.

```bash
git clone https://github.com/x00byte/SparkSpan.git
cd SparkSpan
mkdir -p settings
cp config.example.json settings/config.json
python3 - <<'PYSET'
import json, secrets
from pathlib import Path
p = Path("settings/config.json")
c = json.loads(p.read_text())
c["affinity_secret"] = secrets.token_hex(32)
p.write_text(json.dumps(c, indent=2) + "\n")
PYSET
# Edit backend URLs in settings/config.json to addresses reachable from Docker.
docker compose up --build -d
curl http://localhost:8888/v1/models
curl http://localhost:8888/admin/status
```

The example admits six active requests on each of two backends. This is an operating choice for our tested profile, not a universal optimum. The gateway defaults to an eight-request maximum per backend; set `max_capacity` explicitly if your engine supports a higher admission limit.

Compose publishes port 8888. Keep model servers on different ports if they share the gateway host. `host.docker.internal` resolves to the Docker host in the provided Compose file. For remote servers, use their reachable LAN names or addresses. `settings/` is excluded from Git.

A Python deployment is also available:

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
SPARK_GATEWAY_CONFIG="$PWD/settings/config.json" .venv/bin/uvicorn gateway:app \
  --host 0.0.0.0 --port 8888 --no-access-log --no-proxy-headers \
  --timeout-graceful-shutdown 300
```

Run **one gateway process**. Multiple Uvicorn workers would have separate admission tables. The gateway does not add authentication or TLS. Authorization headers pass through to the selected backend; health and model discovery currently use unauthenticated internal requests. Put authentication at a trusted ingress and restrict direct backend access as appropriate for your network.

## Dashboard

![SparkSpan dashboard: live metrics and throughput history](assets/dashboard.png)

The [dashboard](dashboard/README.md) brings live generation and uncached prefill rates, backend health, model discovery, rolling windows, benchmarks and admission controls together on port **11002**. It uses custom HTML, CSS, JavaScript and chart drawing, with no frontend framework, chart package or external font.

Use Admin to drain a backend or change request limits without restarting the models. Live status distinguishes saved settings from settings actually applied by the gateway. Benchmarks require an explicit target and confirmation; isolated test engines stay outside production aggregates. Viewing the dashboard never sends model inference requests.

The screenshot is a captured interface example. Its displayed rates are a momentary observation, not a controlled benchmark or sustained performance claim. See [measurement](#validation-and-measurement) for the limits of the available GPU comparison.

Install and run from the repository root after creating a private dashboard configuration:

```bash
.venv/bin/pip install -r dashboard/requirements.txt
mkdir -p runtime/dashboard
chmod 700 runtime/dashboard
cp dashboard/config.example.json runtime/dashboard/config.json
# Set gateway_url, the absolute gateway_config path, runtime_dir and allowed_hosts.
SPARKSPAN_DASHBOARD_CONFIG="$PWD/runtime/dashboard/config.json" \
  .venv/bin/python -m uvicorn dashboard.app:app \
  --host 0.0.0.0 --port 11002 --no-access-log --no-proxy-headers
```

The dashboard needs read/write access to the gateway settings directory to save atomic admission changes. It needs no GPU or Docker socket. It is a trusted-LAN application without an account login; [setup, controls and deployment](dashboard/README.md) explain its scope.

## Calling the API

Use a name returned by `/v1/models`. Backend aliases come from each engine's `--served-model-name`; the gateway does not rewrite model names or substitute a different model when one is unavailable.

```python
from openai import OpenAI

client = OpenAI(base_url="http://localhost:8888/v1", api_key="local")
stream = client.chat.completions.create(
    model="model-a",  # Replace with an advertised model name.
    messages=[{"role": "user", "content": "Explain prefix caching."}],
    stream=True,
    extra_headers={"X-Conversation-ID": "conversation-42"},
)
for event in stream:
    if event.choices:
        print(event.choices[0].delta.content or "", end="")
```

Use a stable `X-Conversation-ID`, `X-Session-ID`, `metadata.conversation_id` or `metadata.session_id` for each conversation. Without one, the gateway hashes caller identity, model and the first user turn. Affinity lasts up to one hour in a bounded in-memory table. A restart clears it. Requests with the same session can move when their preferred backend is full or unhealthy; cached prefixes are not transferred.

Responses include `X-Spark-Node`. Supplying that header pins a request to a particular backend for diagnostics, while retaining admission limits. SSE comment keepalives are sent after 15 seconds without upstream data.

## Several models, one endpoint

See [examples/multi-model.json](examples/multi-model.json) for two models on one host and a replica on a second host:

| Backend | Host | Advertised model | Backend limit |
| --- | --- | --- | ---: |
| a-model-a | spark-a | model-a | 4 |
| a-model-b | spark-a | model-b | 2 |
| b-model-a | spark-b | model-a | 6 |

`host_limits` caps the combined request load on each host. Both servers on `spark-a` share a six-request host budget. A request for `model-b` always stays on its compatible backend; `model-a` can use either replica. The optional `models` list filters model names exposed by a backend. Omitting it discovers all names returned by that server.

The host budget does not serialize individual GPU kernels or provide fairness between engines. It is a coarse admission control for model servers whose memory and GPU scheduling you have already configured.

## API and operation

| Endpoint | Behavior |
| --- | --- |
| `POST /v1/chat/completions` | Routed chat, streaming or non-streaming |
| `POST /v1/completions` | Routed completions, streaming or non-streaming |
| `POST /tokenize`, `/detokenize` | Routed to a compatible ready backend |
| `GET /v1/models` | Union of models on ready backends |
| `GET /version` | Version from one ready engine |
| `GET /health` | 200 when at least one backend is healthy |
| `GET /ready` | 200 when every non-draining backend is healthy and at least one accepts work |
| `GET /admin/status` | Per-backend models, load, limits, health and queue |
| `GET /metrics` | Backend metrics labelled by replica, plus gateway metrics |

`/health` does not guarantee availability of every configured model. `/ready` excludes backends explicitly drained for maintenance. `/v1/models` lists only models on backends currently accepting work. Stateful Responses API objects, uploads, audio and arbitrary engine administration endpoints are not routed.

See [operation and deployment](docs/operations.md) for scheduling, failure behavior, live capacity changes and an optional helper that preserves an existing production port when switching between independent replicas and TP=2.

## Validation and measurement

The first GPU deployment used the same Qwen hibrid48 checkpoint on two GB10 machines, each running TP=1, with six gateway admission slots per machine. It served production traffic with streaming, tools, reasoning and vision. Distinct-model routing, shared-host limits and the new draining behavior are covered by local HTTP tests, not by a multi-model GPU benchmark.

We do not yet have a controlled performance comparison with TP=2. An early ten-minute replica observation and a much longer historical TP=2 window were unequal and used different workloads and recipes. They have been retained only as [historical provenance](benchmarks/archive/README.md), not as evidence that one topology is faster. The first short observation also used fabric HTTP, so it is not a no-cable benchmark.

Ordinary 1 Gb/s LAN inference routing was separately verified; the dedicated links were still physically attached. A fresh continuous observation should report its actual elapsed duration, network path, production capacity and any maintenance intervals. [Measurement method](benchmarks/README.md).

```bash
.venv/bin/pip install -r requirements-dev.txt
.venv/bin/pytest -q --ignore=dashboard
# To include dashboard and browser tests:
.venv/bin/pip install -r dashboard/requirements-dev.txt
.venv/bin/playwright install chromium --only-shell
.venv/bin/pytest -q
```

Tests exercise real local HTTP/SSE connections to fake engines, including concurrency, model selection, affinity, shared-host limits, queue bounds, cancellation, failover, live capacity changes, draining/resume, metric labels and scoped deployment recovery. They do not measure GPU throughput. No weights, private production prompts, credentials or local deployment settings are included.

## License

No project license has been selected. Publication does not grant a general license to reuse this code. Dependencies and model weights retain their own terms.

## Credits

[myllmbox's Qwen3.8 Flash Next recipe](https://github.com/myllmbox/qwen38-flash-next-recipe) supplied the inference recipe used for the initial GPU deployment. The measured model was [myllmbox/Qwen3.8-Flash-Next-hibrid48-uncensored](https://huggingface.co/myllmbox/Qwen3.8-Flash-Next-hibrid48-uncensored), based on Qwen. [vLLM](https://github.com/vllm-project/vllm) supplied inference and native metrics; no vLLM engine source is bundled here. [FastAPI](https://github.com/fastapi/fastapi), [HTTPX](https://github.com/encode/httpx) and [Uvicorn](https://github.com/Kludex/uvicorn) provide the HTTP components. [Playwright](https://github.com/microsoft/playwright-python) supplies dashboard browser testing. The dashboard application and frontend were written for SparkSpan; no source from the previous local monitoring dashboard is bundled. The cover is an illustrative image, not a photograph of the measured installation. SparkSpan is an independent project and does not imply endorsement by these projects or hardware vendors.
