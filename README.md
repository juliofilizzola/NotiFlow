# NotiFlow

Serviço de ingestão e processamento assíncrono de eventos de notificação (email, sms, push) em Go — apenas stdlib.

```bash
go test -race ./...
NOTIFLOW_ADDR=:8080 NOTIFLOW_WORKERS=32 go run ./cmd/notiflow
curl -X POST localhost:8080/v1/events -d '{"event_id":"evt-1","user_id":"u1","type":"welcome","channel":"email","data":{"name":"Ana"}}'
curl localhost:8080/metrics
```

## Fluxo de dados

```
            ┌─────────────────────── API (request path: só valida e publica) ───────────────────────┐
 Webhook ──▶│ POST /v1/events ─▶ decode (64KB, campos estritos) ─▶ Validate ─▶ stamp received_at    │
            │        │ 400 inválido            503 + Retry-After se fila cheia      202 Accepted     │
            └────────┼──────────────────────────────────┬────────────────────────────────────────────┘
                     │                                  ▼
                     │                     ┌────────────────────────┐
                     │                     │   Broker: notifications │  (Kafka / RabbitMQ / SQS)
                     │                     └────────────┬───────────┘
                     │                                  │ Deliveries
                     │        ┌─────────────────────────┼─────────────────────────┐
                     │        ▼                         ▼                         ▼
                     │   worker 0                  worker 1      ...        worker N-1      (Worker Pool)
                     │        └──────────────── handle(event) ───────────────────┘
                     │                                  │
                     │          1. Idempotency.Acquire(event_id, ProcessingTTL) ──▶ Redis SET NX PX
                     │                 └─ já existe ─▶ Ack + outcome=duplicate
                     │          2. retry.Do (máx 3 tentativas, backoff exponencial + jitter)
                     │               ├─ RateLimiter.Wait(channel)   token bucket por provedor
                     │               └─ Sender.Send(ctx timeout)    email | sms | push provider
                     │          3a. sucesso ─▶ Idempotency.Complete(CompletedTTL) ─▶ Ack
                     │          3b. permanente / esgotou ─▶ DLQ ─▶ Release(key) ─▶ Ack
                     │          3c. shutdown / store fora ─▶ Release ─▶ Nack(requeue)
                     │                                  │
                     ▼                                  ▼
               /metrics (Prometheus) ◀──────── metrics.Recorder          ┌──────────────┐
                                                                         │  DLQ (+replay)│
                                                                         └──────────────┘
```

## Componentes

| Pacote | Responsabilidade |
|---|---|
| [cmd/notiflow](cmd/notiflow/main.go) | Entrypoint: logger, sinais e `app.New(config.Load()).Run`. |
| [internal/config](internal/config/config.go) | Configuração (env vars, limites por canal, política de retry, TTLs). |
| [internal/app](internal/app/app.go) | Composição das dependências ([app.go](internal/app/app.go)) e ciclo de vida/shutdown gracioso ([run.go](internal/app/run.go)). |
| [internal/contract](internal/contract/interfaces.go) | Todas as interfaces (portas) do serviço: `Publisher`, `Consumer`, `DeadLetterPublisher`, `IdempotencyStore`, `RateLimiter`, `Sender`, `Recorder`. |
| [internal/event](internal/event/event.go) | Contrato do evento e validação de fronteira (retorna todas as violações). |
| [internal/api](internal/api/handler.go) | `POST /v1/events`: valida, publica, `202`. Backpressure via `503 Retry-After`. |
| [internal/broker](internal/broker/broker.go) | Interfaces `Publish` / `Deliveries` / `PublishDLQ` + `Ack`/`Nack`. `Memory` para dev/testes. |
| [internal/worker](internal/worker/pool.go) | Worker Pool e orquestração idempotência → rate limit → retry → ack/DLQ. |
| [internal/retry](internal/retry/retry.go) | `Policy.Backoff`, `Do`, classificação `Permanent` vs transitório. |
| [internal/idempotency](internal/idempotency/store.go) | Lock em duas fases (`processing` → `completed`). |
| [internal/ratelimit](internal/ratelimit/limiter.go) | Token bucket por canal/provedor. |
| [internal/sender](internal/sender/sender.go) | `Router` por canal + provedor simulado. |
| [internal/metrics](internal/metrics/metrics.go) | Exportador Prometheus (texto) que implementa `contract.Recorder`. |

## Decisões de design

**Garantia de entrega.** O broker entrega *at-least-once*; a idempotência transforma isso em *efetivamente uma vez* por `event_id` dentro de `CompletedTTL`. O cliente reenvia com o mesmo `event_id` ao receber 503 — é seguro.

**Idempotência em duas fases.** `Acquire` só reivindica a chave com TTL curto (`ProcessingTTL`). Se o worker morrer, a chave expira e a redelivery do broker é processada; um `SETNX` "permanente" logo de cara perderia a mensagem. `ProcessingTTL` precisa superar o pior caso (`tentativas × AttemptTimeout + backoffs + espera de rate limit`). Se o store estiver fora, o worker *falha fechado* (requeue) em vez de arriscar envio duplicado.

Implementação Redis do `contract.IdempotencyStore`:

```go
func (r *Redis) Acquire(ctx context.Context, key string, ttl time.Duration) (bool, error) {
	return r.c.SetNX(ctx, "idem:"+key, "processing", ttl).Result()
}
func (r *Redis) Complete(ctx context.Context, key string, ttl time.Duration) error {
	return r.c.Set(ctx, "idem:"+key, "completed", ttl).Err()
}
// Release só apaga se ainda estiver "processing" (atômico via Lua).
var releaseScript = redis.NewScript(`if redis.call("GET", KEYS[1]) == "processing" then return redis.call("DEL", KEYS[1]) end return 0`)
```

**Retry.** 3 tentativas no total, `Base × 2^(n-1)` limitado por `MaxDelay`, com *equal jitter* (metade fixa, metade aleatória) para evitar thundering herd quando um provedor volta. Erros embrulhados com `retry.Permanent` (4xx, destinatário inválido, canal sem sender) vão direto para a DLQ. O retry é *in-worker* porque os atrasos são curtos (ms–s); para backoffs de minutos, troque por re-publicação com delay (SQS `DelaySeconds`, RabbitMQ delayed exchange, tópicos `retry-1m`/`retry-10m` no Kafka) para não prender workers.

**DLQ.** Leva o evento original + motivo, nº de tentativas e se foi permanente. Se a publicação na DLQ falhar, a mensagem é *requeued*, nunca ack'ada — nada se perde. Ao entrar na DLQ a chave de idempotência é liberada, permitindo replay após corrigir a causa.

**Rate limiting.** Cada tentativa consome um token do canal antes de chamar o provedor, então retries também respeitam o limite. O bucket é por processo: com N réplicas, use `limite/N` ou mova para Redis (GCRA / `redis_rate`). Combine com particionamento por canal no broker para isolar um provedor lento (um canal saturado não deve consumir todos os workers — com tráfego misto, considere pools separados por canal).

**Shutdown gracioso.** `SIGTERM` → para o HTTP → fecha a fila → workers drenam → após `shutdownTimeout` cancela o contexto; o trabalho interrompido é *requeued* (não vai para a DLQ, a falha não é do evento).

## Observabilidade

Métricas expostas em `GET /metrics`:

| Métrica | Tipo | Labels | Pergunta que responde |
|---|---|---|---|
| `notiflow_events_received_total` | counter | `channel`, `result` (`accepted`/`invalid`/`queue_full`/`publish_error`) | Estamos rejeitando tráfego? |
| `notiflow_delivery_attempts_total` | counter | `channel`, `result` (`success`/`transient_error`/`permanent_error`) | Saúde de cada provedor. |
| `notiflow_events_processed_total` | counter | `channel`, `outcome` (`delivered`/`duplicate`/`dead_lettered`/`requeued`) | Resultado final de negócio. |
| `notiflow_event_processing_duration_seconds` | histogram | `channel`, `outcome` | Latência dequeue → resultado (inclui retries). |
| `notiflow_event_queue_latency_seconds` | histogram | `channel` | Lag da fila — sinal para escalar workers. |
| `notiflow_ratelimit_wait_seconds` | histogram | `channel` | Estamos limitados pelo provedor? |
| `notiflow_worker_inflight` | gauge | — | Saturação do pool. |

Labels têm cardinalidade fechada (enums); **nunca** use `user_id`/`event_id` como label — isso vai para logs (JSON estruturado com `event_id` em toda linha) e traces.

PromQL úteis:

```promql
# Taxa de sucesso por canal (SLO)
sum by (channel) (rate(notiflow_events_processed_total{outcome="delivered"}[5m]))
  / sum by (channel) (rate(notiflow_events_processed_total{outcome!="duplicate"}[5m]))

# p99 de processamento
histogram_quantile(0.99, sum by (le, channel) (rate(notiflow_event_processing_duration_seconds_bucket[5m])))

# Taxa de erro transitório do provedor (alertar antes de virar DLQ)
sum by (channel) (rate(notiflow_delivery_attempts_total{result="transient_error"}[5m]))
  / sum by (channel) (rate(notiflow_delivery_attempts_total[5m]))

# Alerta: qualquer coisa entrando na DLQ
sum by (channel) (increase(notiflow_events_processed_total{outcome="dead_lettered"}[10m])) > 0
```

O exportador é feito à mão para manter zero dependências. Em produção, troque `metrics.Registry` por uma implementação do `contract.Recorder` com `prometheus/client_golang` (mesmos nomes/labels) ou OpenTelemetry — o worker e a API não mudam:

```go
type Prom struct{ processed *prometheus.CounterVec; duration *prometheus.HistogramVec /* ... */ }

func (p *Prom) EventProcessed(ch string, o metrics.Outcome, d time.Duration) {
	p.processed.WithLabelValues(ch, string(o)).Inc()
	p.duration.WithLabelValues(ch, string(o)).Observe(d.Seconds())
}
```

## Para produção

- Trocar `broker.Memory` por um adapter real (Kafka com consumer group / RabbitMQ com `prefetch` = nº de workers) — a interface já modela `Ack`/`Nack`.
- Trocar `idempotency.Memory` (sem limpeza de chaves expiradas, um processo só) por Redis.
- Autenticar webhooks (HMAC do corpo + timestamp) no `api` antes de validar o payload.
- Propagar trace context (OpenTelemetry) do request até o envio, via headers da mensagem.
