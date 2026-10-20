# Continuous production observation

There is no current matched TP=2 versus independent-replica performance result. The first short observation is retained in the [historical archive](archive/README.md) and must not be used as a topology speedup claim.

A fresh observation should begin at a recorded UTC timestamp and continue while production runs. Report actual elapsed time from the first valid metric sample to the latest valid sample, together with both timestamps. Do not label a requested ten-minute run complete merely because ten minutes were configured. Mark interruptions, sample failures, model restarts, counter resets, gateway cutovers and maintenance periods explicitly.

## Metrics

Poll `/admin/status` and `/metrics` at a fixed cadence, for example ten seconds. Save only sanitized operational counters and timing, not prompts, credentials or raw production requests. Native vLLM counters have a `replica` label; track each replica before summing.

For a stable interval without counter resets:

- Aggregate output tokens/s is the sum of each replica's generation-counter delta divided by actual elapsed seconds.
- Uncached prefill tokens/s uses computed prompt tokens, excluding cached input. Prompt-token totals including cache are a different statistic.
- Record completed requests, errors, preemptions, running and waiting requests, gateway queue, admission capacity and memory headroom alongside throughput.
- Calculate latency from matching histogram count/sum deltas or distributions. Do not sum backend latency averages or compare differently weighted averages as equivalent.

If an engine restarts, split the observation at the reset. If a sample fails, disclose the missing interval rather than silently manufacturing a rate. During a drain, retain metrics for the draining engine until its work finishes and record the temporary capacity reduction.

## Comparing recipes or topologies

A fair performance comparison should replay the same prompts, output lengths, sampling parameters, cache preparation and concurrency against each recipe, with the same engine settings where the topology permits. Independent replicas require a full model copy and keep separate caches. TP=2 shares a request across ranks, so its memory allocation and communication costs differ.

For a production observation that cannot replay identical work, publish the workloads and durations as observations without ranking overall performance. Record model/checkpoint revision, engine revision, KV precision and size, block alignment, context limit, speculative settings and prefix-cache behavior. A high aggregate throughput can still hide long per-request stalls or increased time to first token.

## Verifying ordinary LAN inference

Record the actual backend URL path and network interface used for HTTP. Before labelling a window LAN-only, allow older fabric HTTP connections to finish and verify there are no remaining inference sockets on that path. Reused keepalive connections can retain the old destination after a port redirect changes. A physically attached dedicated link can still be used for management; describe this accurately rather than claiming an unplugged-cable test.

## Maintenance windows

When B is drained for model testing, A continues production at its own configured capacity. Keep that period separate from a two-replica steady-state comparison. Exclude synthetic maintenance reservations or load tests from unqualified production throughput summaries. Returning B to service begins a new interval with different capacity and usually a different cache state.

## Credits

[vLLM](https://github.com/vllm-project/vllm) supplies the native engine metrics. The initial GPU deployment used [myllmbox's recipe](https://github.com/myllmbox/qwen38-flash-next-recipe) and [hibrid48 checkpoint](https://huggingface.co/myllmbox/Qwen3.8-Flash-Next-hibrid48-uncensored), based on Qwen. SparkSpan supplies the request-routing state and observation guidance.
