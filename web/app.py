from __future__ import annotations

import asyncio
import hmac
import json
import os
import secrets
from contextlib import asynccontextmanager
from datetime import date
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from .service import DEPTHS, SOURCES, TERMINAL_STATUSES, JobManager, QueueFullError, Settings, validate_payload


ROOT = Path(__file__).resolve().parents[1]
templates = Jinja2Templates(directory=str(ROOT / "web" / "templates"))
SOURCE_LABELS = {"weibo": "微博", "xiaohongshu": "小紅書", "bilibili": "B站", "zhihu": "知乎", "douyin": "抖音", "wechat": "微信", "baidu": "百度", "toutiao": "頭條"}


def safe_href(value: object) -> str:
    url = str(value or "").strip()
    return url if url.lower().startswith(("https://", "http://")) else "#"


def csrf_token(request: Request) -> str:
    token = request.session.get("csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        request.session["csrf_token"] = token
    return token


def verify_csrf(request: Request, token: str | None) -> None:
    expected = request.session.get("csrf_token")
    if not token or not expected or not hmac.compare_digest(token, expected):
        raise HTTPException(status_code=403, detail="CSRF 驗證失敗")


def api_user(request: Request) -> None:
    if not request.session.get("authenticated"):
        raise HTTPException(status_code=401, detail="請先登入")


def page_user(request: Request) -> RedirectResponse | None:
    if not request.session.get("authenticated"):
        return RedirectResponse("/login", status_code=303)
    return None


def job_context(job: dict[str, Any]) -> dict[str, Any]:
    result = dict(job)
    report_path = Path(job["output_dir"]) / "report.json"
    result["report"] = None
    if job["status"] in {"succeeded", "partial"} and report_path.exists():
        try:
            result["report"] = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            result["artifact_error"] = "報告檔案無法讀取"
    return result


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    manager = JobManager(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await manager.start()
        yield
        await manager.stop()

    app = FastAPI(title="last30days-cn 工作台", docs_url=None, redoc_url=None, lifespan=lifespan)
    app.state.manager = manager
    app.state.settings = settings
    app.add_middleware(
        SessionMiddleware,
        secret_key=os.environ.get("SESSION_SECRET") or secrets.token_urlsafe(48),
        https_only=os.environ.get("SESSION_HTTPS_ONLY", "0") == "1",
        same_site="lax",
        session_cookie="last30days_session",
    )
    app.mount("/static", StaticFiles(directory=str(ROOT / "web" / "static")), name="static")
    templates.env.filters["safe_href"] = safe_href

    def render(request: Request, name: str, **context: Any) -> HTMLResponse:
        context.setdefault("csrf_token", csrf_token(request))
        context.setdefault("sources", [(source, SOURCE_LABELS[source]) for source in SOURCES])
        context.setdefault("today", date.today().isoformat())
        return templates.TemplateResponse(request, name, context)

    @app.get("/login", response_class=HTMLResponse)
    async def login_page(request: Request):
        if request.session.get("authenticated"):
            return RedirectResponse("/", status_code=303)
        return render(request, "login.html", configured=bool(os.environ.get("APP_PASSWORD")))

    @app.post("/login")
    async def login(request: Request):
        form = await request.form()
        verify_csrf(request, form.get("csrf_token"))
        password = os.environ.get("APP_PASSWORD")
        if not password:
            return render(request, "login.html", configured=False, error="伺服器尚未設定 APP_PASSWORD")
        if not hmac.compare_digest(str(form.get("password", "")), password):
            return render(request, "login.html", configured=True, error="密碼不正確")
        request.session["authenticated"] = True
        return RedirectResponse("/", status_code=303)

    @app.post("/logout")
    async def logout(request: Request):
        form = await request.form()
        verify_csrf(request, form.get("csrf_token"))
        request.session.clear()
        return RedirectResponse("/login", status_code=303)

    @app.get("/", response_class=HTMLResponse)
    async def home(request: Request):
        redirect = page_user(request)
        if redirect:
            return redirect
        return render(request, "index.html", depths=DEPTHS)

    @app.get("/history", response_class=HTMLResponse)
    async def history(request: Request):
        redirect = page_user(request)
        if redirect:
            return redirect
        return render(request, "history.html", jobs=manager.store.list_recent())

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    async def job_page(request: Request, job_id: str):
        redirect = page_user(request)
        if redirect:
            return redirect
        job = manager.store.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="找不到研究任務")
        return render(request, "job.html", job=job_context(job), source_labels=SOURCE_LABELS)

    async def create_job(request: Request) -> dict[str, Any]:
        api_user(request)
        verify_csrf(request, request.headers.get("X-CSRF-Token"))
        try:
            raw = await request.json()
            payload = validate_payload(raw)
            return await manager.create(payload)
        except json.JSONDecodeError as exc:
            raise HTTPException(status_code=400, detail="請提供 JSON 請求內容") from exc
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except QueueFullError as exc:
            raise HTTPException(status_code=429, detail="研究佇列已滿，請稍後再試") from exc

    @app.post("/api/jobs", status_code=201)
    async def create_job_api(request: Request):
        return await create_job(request)

    @app.get("/api/jobs")
    async def list_jobs(request: Request):
        api_user(request)
        return manager.store.list_recent()

    @app.get("/api/jobs/{job_id}")
    async def get_job(request: Request, job_id: str):
        api_user(request)
        job = manager.store.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="找不到研究任務")
        return job_context(job)

    @app.post("/api/jobs/{job_id}/cancel")
    async def cancel_job(request: Request, job_id: str):
        api_user(request)
        verify_csrf(request, request.headers.get("X-CSRF-Token"))
        job = manager.cancel(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="找不到研究任務")
        return job

    @app.get("/api/jobs/{job_id}/events")
    async def job_events(request: Request, job_id: str):
        api_user(request)
        if not manager.store.get(job_id):
            raise HTTPException(status_code=404, detail="找不到研究任務")

        async def stream():
            previous = None
            while True:
                job = manager.store.get(job_id)
                if not job:
                    return
                snapshot = json.dumps(job, ensure_ascii=False, sort_keys=True)
                if snapshot != previous:
                    yield f"event: update\ndata: {snapshot}\n\n"
                    previous = snapshot
                if job["status"] in TERMINAL_STATUSES:
                    return
                await asyncio.sleep(1)

        return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache"})

    @app.get("/api/jobs/{job_id}/artifacts/{artifact}")
    async def artifact(request: Request, job_id: str, artifact: str):
        api_user(request)
        job = manager.store.get(job_id)
        mapping = {"json": "report.json", "md": "report.md", "html": "report.html"}
        if not job or artifact not in mapping:
            raise HTTPException(status_code=404, detail="找不到報告檔案")
        path = Path(job["output_dir"]) / mapping[artifact]
        if not path.is_file():
            raise HTTPException(status_code=404, detail="報告尚未產生")
        media = "text/html" if artifact == "html" else "application/json" if artifact == "json" else "text/markdown"
        return FileResponse(path, media_type=media, filename=mapping[artifact])

    @app.get("/api/health")
    async def health():
        try:
            manager.store.initialize()
        except OSError as exc:
            return JSONResponse({"status": "error", "detail": str(exc)}, status_code=503)
        disabled = os.environ.get("LAST30DAYS_DISABLE_BROWSER", "0").lower() in {"1", "true", "yes", "on"}
        return {"status": "ok", "browser": "disabled" if disabled else "enabled", "workers": settings.max_workers}

    return app


app = create_app()
