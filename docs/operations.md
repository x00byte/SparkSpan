# Operating SparkSpan

## Dashboard

The optional [control room](../dashboard/README.md) monitors the gateway on port 11002 and provides admission edits, rollback and deliberate benchmarks. It runs as a separate CPU service. Point it at the gateway API and the same gateway settings file; it does not load models or change the production endpoint. The UI labels missing data and stale metrics rather than treating an unreachable backend as idle.

## Scheduling

Admission reserves slots atomically. Each backend is polled about once per second for vLLM running and queued requests, including direct traffic that bypasses SparkSpan. Those observations can briefly lag new direct requests; the model server remains the final scheduler.

The score combines active request count, estimated outstanding prefill work and a preference for a conversation's previous backend. Prefill estimates use JSON request bytes and approximate previous context size. They are routing heuristics, not tokenizer results or a promise of cache hits. Independent servers never share prefix caches.

With the example configuration, up to 32 requests can wait for admission for up to 120 seconds. Full or expired queues return HTTP 429 with `Retry-After`. JSON bodies are limited to 32 MiB each and 128 MiB across buffered requests. There is no FIFO or per-user fairness guarantee. A very large request can still occupy a backend for a long time.

Changing `capacity`, `draining` or `host_limits` in the settings JSON updates admission without restarting the gateway or models. Prefer atomic file replacement. A backend capacity must be between one and its `max_capacity` (eight by default). Malformed updates retain the last valid limits. In-flight requests finish when a limit is reduced. Backend identity, model filters and other settings require a gateway restart.

## Testing on one Spark

The maintenance workflow keeps the production model on A running while B is used for experiments:

1. Atomically set `"draining": true` on B in the mounted settings JSON. Keep its capacity unchanged so resuming restores the intended target. Apply this to every gateway that can still route production to B; old keepalive connections may still reach an older gateway after a cutover.
2. Wait for `/admin/status` to report B as `draining: true` and `accepting: false`. Reload occurs on the polling loop, not at the instant the file changes. Leases reserved before the update continue; unreserved queued requests use another compatible backend, or receive 503 if none remains. Explicit pins do not override draining.
3. Wait for B's `gateway_inflight`, native `running` and native `waiting` to reach zero with a successful fresh poll (`metrics_age_seconds` reports its age). `drained: true` requires those conditions; an unreachable engine is not evidence that it has finished. Confirm zero across at least two fresh polling intervals and ensure there are no direct clients or other gateways sending work.
4. Stop only B's production model using your own scoped launcher. Test a new engine or model there on a separate port or behind restricted access so it cannot accidentally receive production requests. Keep the configured production backend draining throughout.
5. Restore the intended production model and aliases on B, verify its `/v1/models`, and set `"draining": false`. It resumes admission when health and metrics are valid. A's production model does not need a restart.

With two six-slot replicas, draining B reduces the configured production capacity from twelve to six. Queues or latency can increase at the same incoming load. Cached conversations previously on B can move to A and need prefill again. This preserves a running production service; it does not guarantee unchanged performance or eliminate all waiting.

Only stop the engine after drain verification. Gateway health and metrics are sampled and can briefly lag external traffic. A second unconfigured gateway, a direct client, or a newly connected client can invalidate an idle observation. Drain prevents admission through this gateway after its configuration update; it is not a distributed lock on the GPU.

Status distinguishes `healthy` from `accepting`: a healthy draining engine is not accepting new work. `capacity` counts healthy non-draining backends, `configured_capacity` counts all non-draining backends, and `total_configured_capacity` retains the full pool size. Native counters and the engine's existing in-flight requests continue to appear during draining. Prometheus also exposes `spark_gateway_draining`, `spark_gateway_drained` and `spark_gateway_accepting` per replica.

`/ready` requires all non-draining backends to be healthy and at least one backend accepting work. Thus planned B maintenance does not make a healthy A unready. Models available only on B disappear from `/v1/models` while it drains; requests for such a configured model receive 503 and are never substituted with another model. If all backends drain, `/health` and `/ready` return 503.

Gateway changes must themselves be rolled out without cutting active streams: start the new CPU gateway on another port, direct new connections there, and retain the old process until its own active requests finish. Existing HTTP keepalive connections can still assign work through the old instance. Every live gateway needs drain support and the same maintenance setting; a new process cannot retroactively drain an older process. The included endpoint helper accepts only fixed ports 8890 and 8891, so review any staged deployment using another port rather than assuming it is redirected.

## Health and failures

Only a failure to establish an upstream connection is retried on another matching backend. HTTP errors and failures after a response has started are not replayed. This avoids duplicating a generation whose completion is uncertain. Interrupted streams terminate so callers can detect incomplete output.

Health checks, metric collection and model discovery must succeed before a backend becomes usable. New work avoids backends with 180 seconds of unchanged token/cache progress under load. This is a conservative observation, not a diagnosis of GPU health. SparkSpan never restarts a model automatically.

Client disconnection closes the upstream response and releases the reservation, including while queued or waiting for headers. The engine controls when its GPU scheduler notices the closed connection. A shared host budget does not cover traffic to an unconfigured engine or a second gateway process.

Backend metrics remain labelled with `replica`. Sum token counters or their rates across replicas for aggregate throughput; do not sum per-backend averages of latency. On transient outages, the last native metric values remain exposed, while `spark_gateway_backend_up` becomes zero. Consumers must handle stale gauges and normal counter resets after engine restarts.

The Docker configuration runs without GPU devices, uses a read-only root filesystem, drops Linux capabilities and limits CPU and memory. The settings directory is mounted read-only so atomic host-side updates remain visible. The 300-second shutdown grace helps active requests finish; streams longer than that can still be cut off during a gateway stop. Restarting the process loses its in-memory session affinities.

## Adopting existing launchers

For an existing cluster, `deploy.py` can reconcile explicitly named containers. It checks each external launcher's `--validate-assets`, adopts healthy containers using their expected image IDs and starts only missing replicas. It never stops healthy model servers or unrelated containers. Model loading, memory guards and model-specific settings belong to your launcher, outside this repository.

See [deployment.example.json](../examples/deployment.example.json). It describes local and SSH-managed nodes, expected image IDs, launcher paths, API URLs and a CPU gateway image built from this repository. All placeholders must be replaced. Build or load the expected images on the relevant hosts first. The script expects access to Docker on each node.

```bash
python3 deploy.py --deployment /secure/deployment.json --validate-assets
python3 deploy.py --deployment /secure/deployment.json
```

The reconciler's `model_alias` check is designed for the original same-model replica deployment. For different models, start the model servers yourself and use Compose or Uvicorn for the gateway. It does not install an SSH key, launch engine replicas inside Compose, or configure a machine's model selector.

## Keeping an existing production port

The normal Compose installation publishes port 8888 directly and needs no firewall changes. An optional Linux nftables helper, `endpoint_redirect.py`, supports the narrower case where an existing model already occupies port 8888 and clients must retain that URL during an independent-replica test.

The helper redirects new local and incoming IPv4 connections for port 8888 to gateway port 8890. `enable-next` targets 8891 for a controlled gateway replacement. It modifies only the `qwen_hybrid_endpoint` table, checks gateway readiness before enabling, and never flushes unrelated tables. It is a fixed-port, Linux-specific example, not a generic firewall manager.

To use it, review the source and install it root-owned at `/usr/local/sbin/qwen-hybrid-endpoint`. Restrict privilege delegation to its fixed actions; do not make the source or parent directories writable by untrusted users. It depends on `/usr/sbin/nft` and Python 3.

```bash
sudo /usr/local/sbin/qwen-hybrid-endpoint check
sudo /usr/local/sbin/qwen-hybrid-endpoint enable
sudo /usr/local/sbin/qwen-hybrid-endpoint status
sudo /usr/local/sbin/qwen-hybrid-endpoint disable
```

The head backend must listen on an address reachable through `127.0.0.2:8888`; point the gateway and launcher's readiness probe there. This loopback address bypasses the local redirect and prevents recursive proxying. Use the remote backend's ordinary LAN URL for inference; SSH may use another management path.

Already-established connections keep their original destination, including reused HTTP keepalive connections. During cutover, retain the old gateway until its active requests have drained. Do not assume a changed redirect has moved every existing stream. The helper does not persist a system-wide firewall rule across reboot.

Before stopping the independent replicas or bringing TP=2 back to port 8888, call `disable` and let existing requests drain. Integrate this into your own model selector explicitly. The included tests simulate safe rule application and launcher adoption; a TP=2 GPU restart is not part of those tests.

## Credits

[vLLM](https://github.com/vllm-project/vllm) supplies the native health and metrics interfaces. [nftables](https://netfilter.org/projects/nftables/) supplies the optional Linux endpoint redirection mechanism. [Docker Compose](https://docs.docker.com/compose/) supplies the portable gateway service definition.
