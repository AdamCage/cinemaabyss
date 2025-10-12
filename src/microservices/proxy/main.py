
import os
import random
from typing import Optional, Dict, Iterable

import uvicorn
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse, PlainTextResponse, StreamingResponse
import httpx

APP_NAME = "cinemaabyss-proxy-service"

# ---- Env/config ----
PORT = int(os.getenv("PORT", "8000"))
MONOLITH_URL = os.getenv("MONOLITH_URL", "http://monolith:8080")
MOVIES_SERVICE_URL = os.getenv("MOVIES_SERVICE_URL", "http://movies-service:8081")
EVENTS_SERVICE_URL = os.getenv("EVENTS_SERVICE_URL", "http://events-service:8082")
GRADUAL_MIGRATION = os.getenv("GRADUAL_MIGRATION", "true").strip().lower() == "true"
MOVIES_MIGRATION_PERCENT = float(os.getenv("MOVIES_MIGRATION_PERCENT", "50"))  # 0..100

TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "10"))

app = FastAPI(title=APP_NAME)


@app.get("/health")
async def health():
    return {"status": "ok", "service": APP_NAME}


def _filtered_headers(headers: Iterable[tuple]) -> Dict[str, str]:
    """
    Drop hop-by-hop headers that shouldn't be forwarded.
    """
    hop_by_hop = {
        "connection", "keep-alive", "proxy-authenticate", "proxy-authorization",
        "te", "trailers", "transfer-encoding", "upgrade"
    }
    out = {}
    for k, v in headers:
        lk = k.lower()
        if lk not in hop_by_hop:
            out[k] = v
    # overwrite Host just in case
    out.pop("Host", None)
    return out


def choose_movies_backend() -> str:
    """
    Strangler Fig routing decision for the /api/movies domain.
    If GRADUAL_MIGRATION is True -> route X% to MOVIES_SERVICE, (100-X)% to MONOLITH.
    If GRADUAL_MIGRATION is False -> route 100% to MOVIES_SERVICE (full cutover for the domain).
    """
    if not GRADUAL_MIGRATION:
        return MOVIES_SERVICE_URL

    # Gradual: random choice based on percentage (robust to weird values)
    p = max(0.0, min(100.0, MOVIES_MIGRATION_PERCENT))
    roll = random.random() * 100.0
    return MOVIES_SERVICE_URL if roll < p else MONOLITH_URL


async def proxy_request(request: Request, upstream_base: str, upstream_path: Optional[str] = None) -> Response:
    """
    Generic reverse proxy helper.
    - Replays the incoming request to upstream_base + path + query
    - Streams the response back to the client
    """
    path = request.url.path
    if upstream_path is not None:
        # replace the prefix path with desired upstream path
        path = upstream_path

    url = httpx.URL(upstream_base).join(path)
    if request.url.query:
        url = httpx.URL(str(url) + f"?{request.url.query}")

    method = request.method
    headers = _filtered_headers(request.headers.items())
    body = await request.body()

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        try:
            upstream_resp = await client.request(
                method=method,
                url=url,
                headers=headers,
                content=body,
            )
        except httpx.RequestError as e:
            # Graceful error with context
            return JSONResponse(
                status_code=502,
                content={
                    "error": "Bad Gateway",
                    "detail": str(e),
                    "upstream": upstream_base,
                },
            )

    # stream content back
    filtered = [(k, v) for k, v in upstream_resp.headers.items() if k.lower() not in {"content-encoding", "transfer-encoding", "connection"}]
    return Response(
        content=upstream_resp.content,
        status_code=upstream_resp.status_code,
        headers=dict(filtered),
        media_type=upstream_resp.headers.get("content-type")
    )


# ---- Movies domain (Strangler Fig) ----
@app.api_route("/api/movies", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@app.api_route("/api/movies/{subpath:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def movies_proxy(request: Request, subpath: Optional[str] = None):
    upstream = choose_movies_backend()
    upstream_path = request.url.path  # keep same path
    return await proxy_request(request, upstream, upstream_path)


# ---- Events domain (direct to events-service) ----
@app.api_route("/api/events", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
@app.api_route("/api/events/{subpath:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def events_proxy(request: Request, subpath: Optional[str] = None):
    return await proxy_request(request, EVENTS_SERVICE_URL)


# ---- Fallback: send all other /api/* to monolith ----
@app.api_route("/api/{subpath:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])
async def monolith_fallback(request: Request, subpath: Optional[str] = None):
    return await proxy_request(request, MONOLITH_URL)


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT, log_level="info")
