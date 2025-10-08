
# Proxy Service (API Gateway) — FastAPI

Implements Strangler Fig routing for the `/api/movies` domain and proxies other `/api/*` routes to the monolith.
- Gradual migration is controlled by two env vars:
  - `GRADUAL_MIGRATION=true|false`
  - `MOVIES_MIGRATION_PERCENT=0..100`
- When `GRADUAL_MIGRATION=false`, 100% of `/api/movies` traffic goes to `MOVIES_SERVICE_URL`.
- When `GRADUAL_MIGRATION=true`, X% of `/api/movies` requests go to `MOVIES_SERVICE_URL`, the rest to `MONOLITH_URL`.

### Local run
```bash
uvicorn main:app --reload --port 8000
curl http://localhost:8000/health
```

### Example
```bash
# With MOVIES_MIGRATION_PERCENT=80
curl http://localhost:8000/api/movies
```
