from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from prometheus_fastapi_instrumentator import Instrumentator
import httpx, os, time, urllib3, logging

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="AI Platform API Gateway")
Instrumentator().instrument(app).expose(app)

VLLM_URL = os.environ["VLLM_URL"]
QDRANT_URL = os.environ.get("QDRANT_URL", "http://qdrant:6333")


class ChatRequest(BaseModel):
    query: str = Field(..., min_length=1)
    embedding: list[float] = Field(default_factory=lambda: [0.0] * 384)


@app.post("/api/v1/chat")
async def chat(req: ChatRequest):
    start = time.time()

    # 1. Vector search
    async with httpx.AsyncClient(timeout=10, verify=False) as client:
        search_resp = await client.post(
            f"{QDRANT_URL}/collections/documents/points/search",
            json={"vector": req.embedding, "limit": 3}
        )
        context = search_resp.json().get("result", []) if search_resp.status_code == 200 else []

    # 2. LLM inference
    prompt = f"Context: {context}\n\nQuery: {req.query}"
    async with httpx.AsyncClient(timeout=30, verify=False) as client:
        logger.info(f"Calling LLM: {VLLM_URL}/v1/chat/completions")
        llm_resp = await client.post(
            f"{VLLM_URL}/v1/chat/completions",
            json={
                "model": "Qwen/Qwen2.5-7B-Instruct-GPTQ-Int4",
                "messages": [{"role": "user", "content": prompt}]
            }
        )
        logger.info(f"LLM response status: {llm_resp.status_code}, body: {llm_resp.text[:200]}")
        if llm_resp.status_code != 200:
            raise HTTPException(status_code=502, detail=f"LLM inference failed: {llm_resp.text[:200]}")
        result = llm_resp.json()

    latency = (time.time() - start) * 1000
    return {
        "answer": result["choices"][0]["message"]["content"],
        "latency_ms": round(latency, 2),
        "model": result["model"]
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/admin")
def admin():
    raise HTTPException(status_code=401, detail="Unauthorized")
