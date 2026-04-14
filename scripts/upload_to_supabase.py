"""
pricing_audit.json → Supabase Storage 업로드 스크립트입니다.

사내 PC에서 실행하면 최신 분석 결과를 Supabase에 올려서
대시보드가 항상 최신 데이터를 보여주게 합니다.

사용법:
  python3 scripts/upload_to_supabase.py
"""

import json
import sys
from pathlib import Path

try:
    import requests
except ImportError:
    print("❌ requests 패키지가 필요합니다: pip3 install requests")
    sys.exit(1)

# --- 경로 설정 ---
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
ENV_FILE = Path("/Users/jack/dev/gabwoo/견적계산기/.env.local")
AUDIT_JSON = SCRIPT_DIR / "output" / "pricing_audit.json"
OUTPUT_DIR = SCRIPT_DIR / "output"
PERIODS = ["1m", "3m", "6m", "1y"]

# --- .env.local에서 키 읽기 ---
def load_env(env_path: Path) -> dict:
    """간단한 .env 파서입니다."""
    env = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "=" in line:
            key, value = line.split("=", 1)
            env[key.strip()] = value.strip()
    return env


def get_service_role_key(access_token: str, project_ref: str) -> str:
    """Management API로 service_role 키를 가져옵니다."""
    resp = requests.get(
        f"https://api.supabase.com/v1/projects/{project_ref}/api-keys",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if resp.status_code != 200:
        return ""
    for key in resp.json():
        if key["name"] == "service_role":
            return key["api_key"]
    return ""


def upload():
    # 1. 환경 변수 로드
    if not ENV_FILE.exists():
        print(f"❌ .env.local을 찾을 수 없습니다: {ENV_FILE}")
        sys.exit(1)

    env = load_env(ENV_FILE)
    supabase_url = env.get("SUPABASE_URL")
    access_token = env.get("SUPABASE_ACCESS_TOKEN")
    anon_key = env.get("SUPABASE_ANON_KEY")

    if not supabase_url or not anon_key:
        print("❌ SUPABASE_URL 또는 SUPABASE_ANON_KEY가 .env.local에 없습니다.")
        sys.exit(1)

    # 2. service_role 키 가져오기 (버킷 생성·업로드에 필요)
    project_ref = supabase_url.split("//")[1].split(".")[0]
    service_role_key = get_service_role_key(access_token, project_ref) if access_token else ""

    if not service_role_key:
        print("⚠️ service_role 키를 가져올 수 없습니다. anon_key로 시도합니다.")
        auth_key = anon_key
    else:
        print("🔑 service_role 키 확인 완료")
        auth_key = service_role_key

    # 3. 업로드 대상 파일 수집 (기간별 JSON + 하위 호환용 pricing_audit.json)
    files_to_upload = []
    for p in PERIODS:
        fp = OUTPUT_DIR / f"pricing_audit_{p}.json"
        if fp.exists():
            files_to_upload.append(fp)
        else:
            print(f"⚠️ {fp.name} 없음 — 스킵")
    if AUDIT_JSON.exists():
        files_to_upload.append(AUDIT_JSON)

    if not files_to_upload:
        print("❌ 업로드할 JSON 파일이 없습니다. reverse_engineer_pricing.py를 먼저 실행하세요.")
        sys.exit(1)

    print(f"📦 업로드 대상: {len(files_to_upload)}개 파일")
    for fp in files_to_upload:
        print(f"   - {fp.name} ({fp.stat().st_size/1024:.0f}KB)")

    # 4. 버킷 존재 확인 / 생성
    bucket_name = "gmd-data"
    storage_url = f"{supabase_url}/storage/v1"
    headers_admin = {
        "Authorization": f"Bearer {auth_key}",
        "apikey": anon_key,
    }

    resp = requests.get(f"{storage_url}/bucket", headers=headers_admin)
    buckets = [b["id"] for b in resp.json()] if resp.status_code == 200 else []

    if bucket_name not in buckets:
        print(f"📁 '{bucket_name}' 버킷이 없습니다. 생성합니다...")
        create_resp = requests.post(
            f"{storage_url}/bucket",
            headers={**headers_admin, "Content-Type": "application/json"},
            json={"id": bucket_name, "name": bucket_name, "public": True},
        )
        if create_resp.status_code in (200, 201):
            print(f"   ✅ '{bucket_name}' 버킷 생성 완료 (public)")
        else:
            print(f"   ⚠️ 버킷 생성 실패 ({create_resp.status_code}): {create_resp.text}")
            print("   Supabase 대시보드에서 직접 'gmd-data' 버킷을 public으로 만들어주세요.")
    else:
        print(f"📁 '{bucket_name}' 버킷 확인 완료")

    # 5. 파일 업로드 (upsert) — 모든 기간별 파일 순회
    upload_headers = {
        **headers_admin,
        "Content-Type": "application/json",
        "x-upsert": "true",
    }
    success_count = 0
    for fp in files_to_upload:
        file_path_in_bucket = fp.name
        upload_url = f"{storage_url}/object/{bucket_name}/{file_path_in_bucket}"
        with open(fp, "rb") as f:
            upload_resp = requests.post(upload_url, headers=upload_headers, data=f)
        if upload_resp.status_code in (200, 201):
            print(f"   ✅ {fp.name}")
            success_count += 1
        else:
            print(f"   ❌ {fp.name} 실패 ({upload_resp.status_code}): {upload_resp.text}")

    if success_count == len(files_to_upload):
        public_base = f"{supabase_url}/storage/v1/object/public/{bucket_name}/"
        print(f"\n✅ 업로드 성공! ({success_count}개 파일)")
        print(f"   공개 URL prefix: {public_base}")
        print(f"   예: {public_base}pricing_audit_1m.json")
        return public_base
    else:
        print(f"\n❌ 일부 업로드 실패 ({success_count}/{len(files_to_upload)})")
        sys.exit(1)


if __name__ == "__main__":
    upload()
