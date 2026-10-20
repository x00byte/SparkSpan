# Production observations

These records describe an earlier tensor-parallel deployment and a later independent-replica deployment of the same hibrid48 checkpoint on two GB10 machines. They are **not a replay of the same workload**. The observations do not establish that SparkSpan is faster than TP=2.

[Machine-readable aggregate results](production-observations.json) contain no prompts, credentials, private host addresses or request-level production data.

| Measure | Earlier TP=2 | Independent TP=1 replicas |
| --- | ---: | ---: |
| Observation duration | 60,463.67 seconds | 600.00 seconds |
| Generated tokens | 8,353,421 | 67,172 |
| Computed uncached input tokens | 72,617,185 | 1,344,516 |
| Generated tokens / elapsed second | 138.16 | 111.95 |
| Computed uncached input tokens / elapsed second | 1,201.01 | 2,240.86 |
| Completed requests recorded | 10,670 | 81 |
| Errors recorded | 0 | 0 |
| Admission slots | 10 | 6 per replica, 12 total |

The independent window averaged 11.33 running requests. Each replica delivered about 56 output tokens/s. No preemptions were observed in that ten-minute interval. Minimum available memory on the gateway host was 8.12 GiB, so this is not evidence that a larger concurrency target is safe.

## Method

The TP=2 reference is an archived dashboard run lasting about 16.8 hours. Wall-time rates above divide its recorded token totals by its recorded duration. Its dashboard also records a sample-mean output rate of 141.72 tokens/s; that is a different statistic from the wall-time rate and is not substituted in the table.

The independent window sampled the two engines' native cumulative metrics for ten minutes while production requests were active. Rates are summed end-minus-start token counters divided by measured elapsed time. Uncached input excludes cached prompt tokens. Prompt throughput including cached tokens was 15,864.22 tokens/s, which is not the rate at which the GPUs computed new prompt tokens.

Latency values need caution. The historical dashboard sample-mean time to first token was 3.72 seconds. The independent engines' request-weighted means over their window were 10.96 seconds and 8.97 seconds. These different summaries and different workloads do not support a latency improvement claim.

The new deployment admitted more requests and processed more uncached input per second in its observed window, while producing fewer output tokens per second. Model-server recipes also changed: TP=2 used BF16 KV and MTP up to three tokens; each independent replica used FP8 KV, local PLE offload and dynamic MTP up to seven tokens. Prefix cache sizes and block alignment differed. The gateway alone cannot be credited for the observed difference.

## Ordinary LAN versus dedicated interconnect

Independent replicas require HTTP request and response traffic; they do not perform tensor-parallel GPU collectives. We verified an inference HTTP route over the machines' ordinary 1 Gb/s LAN interfaces. The existing dedicated links remained physically attached and available for management. This was not an unplugged-cable test.

The table above predates the LAN cutover and used a fabric IP for inference HTTP. Existing streaming and reused HTTP connections were allowed to drain instead of being interrupted. A clean LAN-only throughput result was not available when these results were prepared, so the fabric numbers must not be presented as a no-cable benchmark.

## Validation boundaries

The real deployment exercised two replicas of one Qwen checkpoint. Worker qualification covered text, eight simultaneous arithmetic checks, tools, reasoning, vision and expected validation errors; a later six-request check passed with the six-slot admission target. A live streaming cancellation closed the upstream request and released the gateway slot. The model containers stayed running during endpoint cutover.

The repository's HTTP tests separately cover distinct model names on one endpoint and several engines sharing a host budget. They use fake engines and do not prove that any particular set of real models fits or performs well on one GPU. No synthetic twelve-request benchmark was inserted into active production traffic.

A controlled next comparison should replay the same prompts, output lengths, sampling, cache preparation and concurrency against both recipes, then repeat over ordinary LAN with all old fabric inference connections closed. Report aggregate output, uncached prefill, latency distributions, per-request output rate, errors and memory headroom together.

## Credits

The measured [hibrid48 checkpoint](https://huggingface.co/myllmbox/Qwen3.8-Flash-Next-hibrid48-uncensored) and [Qwen3.8 Flash Next recipe](https://github.com/myllmbox/qwen38-flash-next-recipe) come from myllmbox, with the model based on Qwen. [vLLM](https://github.com/vllm-project/vllm) supplies inference and native performance counters. SparkSpan supplies the request gateway and the observation summaries; no third-party model weights are included.
