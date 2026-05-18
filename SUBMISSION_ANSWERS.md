# Lab #28 — Câu Trả Lời 5 Câu Hỏi SUBMISSION

Họ và tên: Nguyễn Văn Thức
Mã học viên: 2A202600238

> Trả lời dựa trên kiến trúc thực tế đã build trong repo này. Tham chiếu code/file thật, không lý thuyết suông.

---

## Câu 1 — Trade-offs trong thiết kế AI platform

Cân bằng 3 yếu tố theo trục **performance / reliability / maintainability**:

| Quyết định | Đánh đổi |
|---|---|
| **Mock vLLM local làm fallback** ([`mock-vllm/`](mock-vllm/)) thay vì only-Kaggle | Hy sinh chất lượng output (Qwen 7B thật vs canned response) để đổi lấy **reliability tuyệt đối** — smoke test không bao giờ flaky vì Kaggle session chết. Chấp nhận được vì lab này chấm Integration Completeness 40%, không chấm LLM quality. |
| **sentence-transformers chạy CPU** ([`embed-service/`](embed-service/)) thay vì GPU | Chậm hơn ~10x (50ms vs 5ms/embed) nhưng **maintainability** cao: container CPU bất kỳ máy nào cũng chạy, không phụ thuộc CUDA version. 384-dim với batch nhỏ không phải bottleneck. |
| **Delta Lake bằng parquet trên local volume** ([`prefect/flows/kafka_to_delta.py:37`](prefect/flows/kafka_to_delta.py#L37)) thay vì Delta Lake thật | Mất ACID transactions + time travel — **performance** ghi nhanh hơn, **maintainability** dependency nhẹ (không cần `deltalake` Rust binding). Khi scale lên production sẽ swap sang Delta Lake hoặc Iceberg. |
| **Prefect dùng work pool `process` không Docker** ([`docker-compose.yml:40`](docker-compose.yml#L40)) | Mất isolation giữa các flow runs — bù lại **maintainability** cao vì không phải mount `/var/run/docker.sock` và lo permission. Reliability tốt cho dev, không phù hợp cho prod multi-tenant. |
| **API Gateway kiểm tra Pydantic ở boundary** ([`api-gateway/main.py:24`](api-gateway/main.py#L24)) | Tốn ~1ms validation overhead — đổi lấy **reliability** (request thiếu field trả 422 thay vì 500) và **maintainability** (schema rõ ràng cho client). |

**Quy tắc tổng**: reliability > maintainability > performance. Performance chỉ tối ưu khi đo được bottleneck thật (Prometheus `http_request_duration_seconds`), không tối ưu phỏng đoán.

---

## Câu 2 — Xử lý ngắt kết nối Local ↔ Kaggle, cơ chế fallback

**Có cơ chế fallback hai-tầng được implement thực sự** (không phải concept):

### Tầng 1 — Mock vLLM container ([`mock-vllm/main.py`](mock-vllm/main.py))

Một container chạy local expose endpoint OpenAI-compatible:
- `GET /v1/models`
- `POST /v1/chat/completions`

trả response cấu trúc giống vLLM thật. API Gateway không phân biệt được gọi mock hay real — cùng wire-format.

### Tầng 2 — Cấu hình ENV trỏ đến đâu thì gọi đó

[`docker-compose.yml:80`](docker-compose.yml#L80):
```yaml
VLLM_URL: ${VLLM_NGROK_URL:-http://mock-vllm:8001}
```

- Khi Kaggle tunnel sống → `.env` set `VLLM_NGROK_URL=https://xxxx.ngrok-free.app` → route Kaggle GPU
- Khi Kaggle tunnel chết / không có → mặc định trỏ `http://mock-vllm:8001` → degrade nhẹ nhàng

Switch giữa hai mode chỉ cần đổi 1 dòng `.env` và `docker compose up -d --force-recreate api-gateway` (~3s). Không redeploy stack, không downtime cho downstream services.

### Cảnh báo còn thiếu

Hiện tại API Gateway **không** auto-detect Kaggle tunnel chết để tự fail-over. Để làm đầy đủ cần:
- Healthcheck định kỳ tới `VLLM_URL/v1/models`
- Circuit breaker (ví dụ `pybreaker`) sau N lỗi liên tiếp thì lật `VLLM_URL` về mock
- Hoặc dùng service mesh / sidecar (Envoy retry policy)

Đây là known gap, scope vượt lab 2h.

---

## Câu 3 — Event-driven architecture với Kafka decouple components

Kafka là **producer-broker-consumer** decoupling theo 3 trục:

### Decouple thời gian (temporal)
- [`scripts/01_ingest_to_kafka.py`](scripts/01_ingest_to_kafka.py) ghi vào topic `data.raw` ngay khi có dữ liệu — **không cần biết** consumer có online hay không.
- [`prefect/flows/kafka_to_delta.py`](prefect/flows/kafka_to_delta.py) chạy theo lịch của Prefect, consume tại thời điểm khác.
- Nếu Prefect worker chết 30 phút → producer vẫn ingest bình thường → khi worker quay lại consume `auto_offset_reset="earliest"` không mất message nào.

### Decouple công nghệ (technology)
- Producer: Python `kafka-python` library
- Consumer: Prefect 2.14 task (cũng kafka-python nhưng có thể là Spark, Flink, Go consumer, etc.)
- Schema chung là JSON — không ràng buộc framework. Có thể đổi 1 bên mà không động bên kia.

### Decouple fan-out (multi-consumer)
- Hiện chỉ có 1 consumer (Prefect → Delta Lake). Nhưng do Kafka **không xóa** message sau khi đọc (giữ theo retention), ta có thể thêm:
  - Realtime consumer push thẳng vào Qdrant
  - Analytics consumer đẩy vào BigQuery
  - Audit consumer log mọi event vào S3
  cùng đọc 1 topic mà **không ảnh hưởng** consumer cũ.

### Lợi ích cụ thể trong lab này
- **Backpressure**: nếu Qdrant chậm, không làm nghẽn upstream producer. Kafka là buffer.
- **Replay**: có thể xóa Delta Lake + Qdrant rồi rerun pipeline → reset state vì Kafka giữ raw events.
- **Observability dễ**: chỉ cần đo offset lag (`docker exec ... kafka-consumer-groups --describe`) để biết pipeline có đuổi kịp hay không, không cần probe từng stage.

---

## Câu 4 — Implementation observability: logs, metrics, traces

**3 trục observability đầy đủ:**

### Metrics (Prometheus + Grafana)
- [`api-gateway/main.py:9`](api-gateway/main.py#L9) dùng `prometheus-fastapi-instrumentator` expose `/metrics` với:
  - `http_requests_total{handler,method,status}` — RED method: Rate / Errors / Duration
  - `http_request_duration_seconds_bucket` — histogram cho percentile latency
- [`monitoring/prometheus.yml`](monitoring/prometheus.yml) scrape api-gateway mỗi 15s
- Grafana datasource Prometheus, query trực tiếp: ví dụ `histogram_quantile(0.95, rate(http_request_duration_seconds_bucket[5m]))` cho p95 latency

### Traces (LangSmith)
- [`api-gateway/main.py:69`](api-gateway/main.py#L69): chat function được wrap bằng `@traceable(run_type="chain", name="rag_chat", project_name="lab28-platform")` từ `langsmith.run_helpers`
- Khi `LANGCHAIN_API_KEY` set, mỗi POST `/api/v1/chat` tự động:
  1. Tạo run trong LangSmith project `lab28-platform`
  2. Log inputs (`query`, `vector`), outputs (`answer`, `latency_ms`, `retrieved_docs`)
  3. Tự động tracing nested calls (vector_search, llm_complete) như spans con
- Verify: 8+ traces đã ghi sau 6 chat calls — xem [`submission-evidence/observability_verify_output.txt`](submission-evidence/observability_verify_output.txt)

### Logs
- Container stdout → `docker compose logs <service>` (default driver `json-file`)
- Prefect task logs → Prefect UI flow run page (UI render từ Postgres metadata)
- Để production cần: ship sang Loki/ELK, structured JSON logs với correlation_id

### Visualization
- Prefect UI (http://localhost:4200): flow-level — run status, duration, log dòng-by-dòng
- Grafana (http://localhost:3000): service-level — RED metrics, SLO panels
- LangSmith (smith.langchain.com): request-level — drill xuống từng RAG call, prompt + response + token cost
- Prometheus UI (http://localhost:9090): ad-hoc PromQL exploration

### Gaps đã biết
- Chưa có distributed tracing toàn-stack (OpenTelemetry → Jaeger/Tempo)
- Logs chưa structured JSON, khó query cross-service
- Chưa wire Loki vào Grafana

---

## Câu 5 — Service crash, graceful degradation

Phân tích từng service crash và behavior thực tế của stack:

| Service chết | Hậu quả | Có graceful degradation? |
|---|---|---|
| **mock-vllm** | API Gateway POST `/api/v1/chat` → 500 (HTTPX ConnectError) | ❌ **Không** — hiện tại không có try/except. Cần thêm circuit breaker + cached response. |
| **Qdrant** | API Gateway vẫn trả 200 với `retrieved_docs: 0` ✅ | ✅ **Có** — [`api-gateway/main.py:39`](api-gateway/main.py#L39) wrap vector search trong try/except, return empty context. RAG xuống cấp thành "answer without retrieval". |
| **Kafka** | Producer scripts fail (`KafkaTimeoutError`); Prefect flow stuck ở consume timeout 5s rồi return 0 records | ⚠️ **Một phần** — flow không crash mà tự log "No records to save" ([`kafka_to_delta.py:30`](prefect/flows/kafka_to_delta.py#L30)). API Gateway không phụ thuộc Kafka realtime nên không ảnh hưởng. |
| **Redis** | `03_delta_to_feast.py` script fail. API Gateway không gọi Redis trực tiếp ở chat endpoint nên chat vẫn OK. | ✅ **Có** (gián tiếp) — feature store ra ngoài critical path của inference. |
| **Prefect Orion** | Worker không thể fetch deployment, flow runs queued. API Gateway + chat hoàn toàn không ảnh hưởng. | ✅ **Có** — pipeline async, không block synchronous traffic. |
| **Prometheus / Grafana** | Mất observability nhưng business logic không ảnh hưởng. Smoke test `test_prometheus_scrapes_api_gateway` fail. | ✅ **Có** — observability là sidecar, không phải critical path. |
| **embed-service** | Script `05_embed_to_qdrant.py` fail. API Gateway không gọi embed-service (client tự gửi vector). | ✅ **Có** — embed-service chỉ trong batch path, không trong serving path. |

### Cải thiện đề xuất (chưa implement)

1. **Retry với exponential backoff** ở vector_search và llm_complete (dùng `tenacity`).
2. **Circuit breaker** cho VLLM_URL — sau 5 lỗi/30s thì mở mạch, return cached/canned response.
3. **Healthcheck endpoint** `/health` đang trả 200 cho dù downstream chết. Cần check sống của Qdrant + vLLM rồi trả `ready: false` để load balancer không route traffic vào (k8s-style readiness probe).
4. **Replica + load balancer** cho mock-vllm và embed-service (compose `deploy.replicas: 2` + nginx upstream).

### Tóm lại

Stack hiện đã có degradation **một phần**: Qdrant, Kafka, Redis, Prefect, monitoring đều graceful. **Điểm yếu duy nhất là vLLM** — vì là critical path duy nhất không có alternative. Đây cũng là lý do mock-vllm tồn tại: nó chính là fallback cho VLLM thật.
