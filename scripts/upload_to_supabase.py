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

    # 3. JSON 파일 확인
    if not AUDIT_JSON.exists():
        print(f"❌ pricing_audit.json을 찾을 수 없습니다: {AUDIT_JSON}")
        print("   먼저 reverse_engineer_pricing.py를 실행해주세요.")
        sys.exit(1)

    file_size = AUDIT_JSON.stat().st_size / 1024
    print(f"📦 업로드 파일: {AUDIT_JSON.name} ({file_size:.0f}KB)")

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

    # 5. 파일 업로드 (upsert)
    file_path_in_bucket = "pricing_audit.json"
    upload_url = f"{storage_url}/object/{bucket_name}/{file_path_in_bucket}"

    with open(AUDIT_JSON, "rb") as f:
        upload_headers = {
            **headers_admin,
            "Content-Type": "application/json",
            "x-upsert": "true",
        }
        upload_resp = requests.post(upload_url, headers=upload_headers, data=f)

    if upload_resp.status_code in (200, 201):
        public_url = f"{supabase_url}/storage/v1/object/public/{bucket_name}/{file_path_in_bucket}"
        print(f"✅ 업로드 성공!")
        print(f"   공개 URL: {public_url}")
        print(f"   대시보드에서 이 URL로 데이터를 fetch합니다.")
        return public_url
    else:
        print(f"❌ 업로드 실패 ({upload_resp.status_code}): {upload_resp.text}")
        sys.exit(1)


if __name__ == "__main__":
    upload()
