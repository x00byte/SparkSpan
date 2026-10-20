# Operating SparkSpan

## Scheduling

Admission reserves slots atomically. Each backend is polled about once per second for vLLM running and queued requests, including direct traffic that bypasses SparkSpan. Those observations can briefly lag new direct requests; the model server remains the final scheduler.

The score combines active request count, estimated outstanding prefill work and a preference for a conversation's previous backend. Prefill estimates use JSON request bytes and approximate previous context size. They are routing heuristics, not tokenizer results or a promise of cache hits. Independent servers never share prefix caches.

With the example configuration, up to 32 requests can wait for admission for up to 120 seconds. Full or expired queues return HTTP 429 with `Retry-After`. JSON bodies are limited to 32 MiB each and 128 MiB across buffered requests. There is no FIFO or per-user fairness guarantee. A very large request can still occupy a backend for a long time.

Changing `capacity` or `host_limits` in the settings JSON updates admission without restarting the gateway or models. Prefer atomic file replacement. A backend capacity must be between one and its `max_capacity` (eight by default). Malformed updates retain the last valid limits. In-flight requests finish when a limit is reduced. Backend identity, model filters and other settings require a gateway restart.

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
