from __future__ import annotations

import asyncio
import json
import os
import shutil
import sqlite3
import sys
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any


SOURCES = ("weibo", "xiaohongshu", "bilibili", "zhihu", "douyin", "wechat", "baidu", "toutiao")
DEPTHS = ("quick", "default", "deep")
TERMINAL_STATUSES = {"succeeded", "partial", "failed", "cancelled"}


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    cli_path: Path
    max_workers: int = 2
    max_queued: int = 10
    retention_days: int = 30

    @property
    def jobs_dir(self) -> Path:
        return self.data_dir / "jobs"

    @property
    def cache_dir(self) -> Path:
        return self.data_dir / "cache"

    @property
    def database_path(self) -> Path:
        return self.data_dir / "app.db"

    @classmethod
    def from_env(cls) -> "Settings":
        root = Path(__file__).resolve().parents[1]
        return cls(
            data_dir=Path(os.environ.get("LAST30_WEB_DATA_DIR", root / "data")),
            cli_path=Path(os.environ.get("LAST30_WEB_CLI_PATH", root / "scripts" / "last30days.py")),
            max_workers=int(os.environ.get("MAX_CONCURRENT_JOBS", "2")),
            max_queued=int(os.environ.get("MAX_QUEUED_JOBS", "10")),
            retention_days=int(os.environ.get("RETENTION_DAYS", "30")),
        )


class JobStore:
    def __init__(self, settings: Settings):
        self.settings = settings

    def initialize(self) -> None:
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        self.settings.jobs_dir.mkdir(parents=True, exist_ok=True)
        self.settings.cache_dir.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY, topic TEXT NOT NULL, days INTEGER NOT NULL,
                    as_of TEXT NOT NULL, depth TEXT NOT NULL, sources TEXT,
                    refresh INTEGER NOT NULL, status TEXT NOT NULL, progress INTEGER NOT NULL,
                    message TEXT NOT NULL, created_at TEXT NOT NULL, started_at TEXT,
                    finished_at TEXT, output_dir TEXT NOT NULL, error TEXT
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.settings.database_path)
        conn.row_factory = sqlite3.Row
        return conn

    @staticmethod
    def _serialize(row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        item = dict(row)
        item["sources"] = json.loads(item["sources"]) if item["sources"] else []
        item["refresh"] = bool(item["refresh"])
        return item

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        job_id = str(uuid.uuid4())
        now = utcnow()
        output_dir = self.settings.jobs_dir / job_id
        with self._connect() as conn:
            queued = conn.execute("SELECT COUNT(*) FROM jobs WHERE status = 'queued'").fetchone()[0]
            if queued >= self.settings.max_queued:
                raise QueueFullError()
            conn.execute(
                """INSERT INTO jobs VALUES (?, ?, ?, ?, ?, ?, ?, 'queued', 0, ?, ?, NULL, NULL, ?, NULL)""",
                (
                    job_id, payload["topic"], payload["days"], payload["as_of"], payload["depth"],
                    json.dumps(payload["sources"]), int(payload["refresh"]), "等待執行", now, str(output_dir),
                ),
            )
        return self.get(job_id)  # type: ignore[return-value]

    def get(self, job_id: str) -> dict[str, Any] | None:
        with self._connect() as conn:
            return self._serialize(conn.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone())

    def list_recent(self) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.settings.retention_days)).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE created_at >= ? ORDER BY created_at DESC", (cutoff,)
            ).fetchall()
        return [self._serialize(row) for row in rows]  # type: ignore[list-item]

    def update(self, job_id: str, **fields: Any) -> None:
        if not fields:
            return
        allowed = {"status", "progress", "message", "started_at", "finished_at", "error"}
        if not set(fields).issubset(allowed):
            raise ValueError("Unsupported job field")
        clauses = ", ".join(f"{key} = ?" for key in fields)
        with self._connect() as conn:
            conn.execute(f"UPDATE jobs SET {clauses} WHERE id = ?", (*fields.values(), job_id))

    def recover(self) -> list[str]:
        with self._connect() as conn:
            conn.execute(
                """UPDATE jobs SET status = 'failed', finished_at = ?, message = ?, error = ?
                   WHERE status = 'running'""",
                (utcnow(), "服務重新啟動，原研究程序已中斷", "服務重新啟動時研究程序中斷"),
            )
            rows = conn.execute("SELECT id FROM jobs WHERE status = 'queued' ORDER BY created_at").fetchall()
        return [row["id"] for row in rows]

    def expire(self) -> list[dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=self.settings.retention_days)).isoformat()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE created_at < ? AND status NOT IN ('queued', 'running')", (cutoff,)
            ).fetchall()
            conn.execute(
                "DELETE FROM jobs WHERE created_at < ? AND status NOT IN ('queued', 'running')", (cutoff,)
            )
        return [self._serialize(row) for row in rows]  # type: ignore[list-item]


class QueueFullError(Exception):
    pass


class JobManager:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.store = JobStore(settings)
        self.queue: asyncio.Queue[str] = asyncio.Queue()
        self.processes: dict[str, asyncio.subprocess.Process] = {}
        self.workers: list[asyncio.Task[None]] = []
        self.maintenance: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.store.initialize()
        for job_id in self.store.recover():
            self.queue.put_nowait(job_id)
        self.clean_expired()
        self.workers = [asyncio.create_task(self._worker()) for _ in range(self.settings.max_workers)]
        self.maintenance = asyncio.create_task(self._maintenance_loop())

    async def stop(self) -> None:
        for process in list(self.processes.values()):
            if process.returncode is None:
                process.terminate()
        for task in [*self.workers, self.maintenance]:
            if task:
                task.cancel()
        await asyncio.gather(*self.workers, *( [self.maintenance] if self.maintenance else [] ), return_exceptions=True)

    async def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        job = self.store.create(payload)
        await self.queue.put(job["id"])
        return job

    def cancel(self, job_id: str) -> dict[str, Any] | None:
        job = self.store.get(job_id)
        if not job:
            return None
        if job["status"] in TERMINAL_STATUSES:
            return job
        self.store.update(job_id, status="cancelled", progress=job["progress"], message="已取消", finished_at=utcnow())
        process = self.processes.get(job_id)
        if process and process.returncode is None:
            process.terminate()
        return self.store.get(job_id)

    def clean_expired(self) -> None:
        jobs_root = self.settings.jobs_dir.resolve()
        for job in self.store.expire():
            output_dir = Path(job["output_dir"])
            try:
                if output_dir.resolve().parent == jobs_root and output_dir.exists():
                    shutil.rmtree(output_dir)
            except OSError:
                pass

    async def _maintenance_loop(self) -> None:
        while True:
            await asyncio.sleep(24 * 60 * 60)
            self.clean_expired()

    async def _worker(self) -> None:
        while True:
            job_id = await self.queue.get()
            try:
                job = self.store.get(job_id)
                if job and job["status"] == "queued":
                    await self._run(job)
            finally:
                self.queue.task_done()

    async def _run(self, job: dict[str, Any]) -> None:
        job_id = job["id"]
        output_dir = Path(job["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        self.store.update(job_id, status="running", progress=5, message="正在啟動研究", started_at=utcnow())
        env = os.environ.copy()
        env.update({
            "LAST30DAYS_OUTPUT_DIR": str(output_dir),
            "LAST30DAYS_CACHE_DIR": str(self.settings.cache_dir),
            "LAST30DAYS_CN_CONFIG_DIR": "",
            "LAST30DAYS_COOKIE_DIR": str(self.settings.data_dir / "browser_cookies"),
            "LAST30DAYS_DISABLE_BROWSER": env.get("LAST30DAYS_DISABLE_BROWSER", "0"),
        })
        command = self._command(job)
        stderr_lines: list[str] = []
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
            )
        except OSError as exc:
            self.store.update(job_id, status="failed", progress=100, message="無法啟動研究程序", finished_at=utcnow(), error=str(exc))
            return
        self.processes[job_id] = process
        stdout_task = asyncio.create_task(process.stdout.read())  # type: ignore[union-attr]

        async def consume_stderr() -> None:
            while True:
                line = await process.stderr.readline()  # type: ignore[union-attr]
                if not line:
                    return
                text = line.decode("utf-8", "replace").strip()
                if text:
                    stderr_lines.append(text)
                    progress, message = progress_from_log(text)
                    self.store.update(job_id, progress=progress, message=message)

        stderr_task = asyncio.create_task(consume_stderr())
        await process.wait()
        await stdout_task
        await stderr_task
        self.processes.pop(job_id, None)

        current = self.store.get(job_id)
        if current and current["status"] == "cancelled":
            return
        report_path = output_dir / "report.json"
        if process.returncode != 0 or not report_path.exists():
            error = "\n".join(stderr_lines[-8:]) or f"研究程序結束，退出碼 {process.returncode}"
            self.store.update(job_id, status="failed", progress=100, message="研究失敗", finished_at=utcnow(), error=error)
            return
        try:
            report = json.loads(report_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            self.store.update(job_id, status="failed", progress=100, message="報告讀取失敗", finished_at=utcnow(), error=str(exc))
            return
        errors = [str(value) for key, value in report.items() if key.endswith("_error") and value]
        status = "partial" if errors else "succeeded"
        self.store.update(
            job_id, status=status, progress=100,
            message="研究完成" if status == "succeeded" else "研究完成，部分來源不可用",
            finished_at=utcnow(), error="\n".join(errors) if errors else None,
        )

    def _command(self, job: dict[str, Any]) -> list[str]:
        command = [sys.executable, str(self.settings.cli_path), job["topic"], "--emit", "json", "--days", str(job["days"])]
        if job["depth"] == "quick":
            command.append("--quick")
        elif job["depth"] == "deep":
            command.append("--deep")
        if job["as_of"]:
            command.extend(["--as-of", job["as_of"]])
        if job["sources"]:
            command.extend(["--search", ",".join(job["sources"])])
        if job["refresh"]:
            command.append("--refresh")
        return command


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def progress_from_log(line: str) -> tuple[int, str]:
    source_names = {
        "微博": "微博", "小红书": "小紅書", "B站": "B站", "知乎": "知乎", "抖音": "抖音", "微信": "微信", "百度": "百度", "头条": "頭條",
        "weibo": "微博", "xiaohongshu": "小紅書", "bilibili": "B站", "zhihu": "知乎", "douyin": "抖音", "wechat": "微信", "baidu": "百度", "toutiao": "頭條",
    }
    for key, label in source_names.items():
        if key in line:
            return 15, f"正在搜尋{label}"
    if "正在处理结果" in line:
        return 85, "正在整理與評分結果"
    if "完成" in line:
        return 95, "正在建立報告"
    return 10, "正在執行研究"


def validate_payload(raw: dict[str, Any]) -> dict[str, Any]:
    topic = str(raw.get("topic", "")).strip()
    if not 1 <= len(topic) <= 200:
        raise ValueError("研究主題必須為 1 至 200 個字元")
    try:
        days = int(raw.get("days", 30))
    except (TypeError, ValueError) as exc:
        raise ValueError("回溯天數必須是整數") from exc
    if not 1 <= days <= 30:
        raise ValueError("回溯天數必須介於 1 至 30")
    depth = str(raw.get("depth", "default"))
    if depth not in DEPTHS:
        raise ValueError("未知的研究模式")
    as_of = str(raw.get("as_of") or date.today().isoformat())
    try:
        parsed_as_of = date.fromisoformat(as_of)
    except ValueError as exc:
        raise ValueError("截止日期必須是 YYYY-MM-DD") from exc
    if parsed_as_of > date.today():
        raise ValueError("截止日期不可晚於今天")
    sources = raw.get("sources") or []
    if not isinstance(sources, list) or any(source not in SOURCES for source in sources):
        raise ValueError("包含未知的資料來源")
    return {"topic": topic, "days": days, "depth": depth, "as_of": as_of, "sources": sources, "refresh": bool(raw.get("refresh", False))}
