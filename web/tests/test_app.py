import time
from pathlib import Path

from fastapi.testclient import TestClient

from web.app import create_app, safe_href
from web.service import Settings


def make_client(tmp_path: Path, monkeypatch) -> TestClient:
    fake_cli = tmp_path / "fake_cli.py"
    fake_cli.write_text(
        """import json, os, pathlib, sys
out = pathlib.Path(os.environ['LAST30DAYS_OUTPUT_DIR']); out.mkdir(parents=True, exist_ok=True)
print('[微博] 搜索中...', file=sys.stderr)
report = {'topic': sys.argv[1], 'range': {'from': '2026-01-01', 'to': '2026-01-30'}, 'generated_at': '2026-01-30T00:00:00Z', 'mode': 'all', 'weibo': [{'title': '可信結果', 'url': 'https://example.com', 'score': 80}], 'xiaohongshu': [], 'bilibili': [], 'zhihu': [], 'douyin': [], 'wechat': [], 'baidu': [], 'toutiao': [], 'zhihu_error': '來源暫時不可用'}
(out / 'report.json').write_text(json.dumps(report), encoding='utf-8')
(out / 'report.md').write_text('# report', encoding='utf-8')
(out / 'report.html').write_text('<h1>report</h1>', encoding='utf-8')
print('完成!', file=sys.stderr)
""",
        encoding="utf-8",
    )
    monkeypatch.setenv("APP_PASSWORD", "test-password")
    monkeypatch.setenv("SESSION_SECRET", "test-session-secret")
    settings = Settings(data_dir=tmp_path / "data", cli_path=fake_cli, max_workers=1, max_queued=1, retention_days=30)
    return TestClient(create_app(settings))


def login(client: TestClient) -> str:
    page = client.get("/login")
    token = page.text.split('name="csrf_token" value="')[1].split('"')[0]
    response = client.post("/login", data={"password": "test-password", "csrf_token": token}, follow_redirects=False)
    assert response.status_code == 303
    return client.get("/").text.split('id="csrf-token" value="')[1].split('"')[0]


def test_login_csrf_and_research_lifecycle(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        assert client.get("/api/jobs").status_code == 401
        csrf = login(client)
        response = client.post("/api/jobs", headers={"X-CSRF-Token": csrf}, json={"topic": "含空格與 ' 引號 的主題", "days": 7, "sources": ["weibo"]})
        assert response.status_code == 201
        job_id = response.json()["id"]
        for _ in range(30):
            job = client.get(f"/api/jobs/{job_id}").json()
            if job["status"] in {"partial", "succeeded", "failed"}:
                break
            time.sleep(0.05)
        assert job["status"] == "partial"
        assert client.get(f"/api/jobs/{job_id}/artifacts/json").status_code == 200
        assert "可信結果" in client.get(f"/jobs/{job_id}").text


def test_api_rejects_missing_csrf(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        login(client)
        response = client.post("/api/jobs", json={"topic": "test", "sources": []})
        assert response.status_code == 403


def test_external_links_only_allow_http_protocols():
    assert safe_href("https://example.com") == "https://example.com"
    assert safe_href("javascript:alert(1)") == "#"
