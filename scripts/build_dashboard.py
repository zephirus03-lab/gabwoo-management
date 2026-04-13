"""
dashboard.html + pricing_audit.json → web/sales-dashboard.html 빌드 스크립트입니다.

JSON 데이터를 HTML 안에 인라인 삽입하여 단일 파일로 배포 가능하게 만듭니다.
Vercel 정적 배포에서 fetch() 없이 바로 동작합니다.
"""

import json
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DASHBOARD_HTML = SCRIPT_DIR / "output" / "dashboard.html"
AUDIT_JSON = SCRIPT_DIR / "output" / "pricing_audit.json"
OUTPUT_HTML = SCRIPT_DIR.parent / "web" / "sales-dashboard.html"


def build():
    # 1. 원본 HTML 로드
    html = DASHBOARD_HTML.read_text(encoding="utf-8")

    # 2. JSON 데이터 로드
    with open(AUDIT_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    json_str = json.dumps(data, ensure_ascii=False)

    # 3. fetch() 호출을 인라인 데이터로 교체
    old_load = """async function loadData() {
  try {
    const resp = await fetch('./pricing_audit.json');
    DATA = await resp.json();
  } catch(e) {
    document.getElementById('tab-overview').innerHTML = '<div style="padding:60px;text-align:center;color:#999;">pricing_audit.json을 같은 폴더에 두고 열어주세요.</div>';
    return;
  }
  renderAll();
}"""

    new_load = f"""async function loadData() {{
  DATA = {json_str};
  renderAll();
}}"""

    if old_load not in html:
        print("⚠️  fetch() 패턴을 찾을 수 없습니다. HTML 구조를 확인하세요.")
        return

    html = html.replace(old_load, new_load)

    # 4. 메인 대시보드로 돌아가는 링크 추가 (헤더 아래)
    back_link = '<div style="background:#16213e;padding:10px 32px;"><a href="index.html" style="display:inline-flex;align-items:center;gap:8px;padding:6px 16px;background:rgba(59,130,246,0.15);border:1px solid rgba(59,130,246,0.3);color:#60a5fa;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none;">📊 경영 대시보드로 돌아가기</a></div>'
    html = html.replace('</header>', f'</header>\n{back_link}')

    # 5. 저장
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")

    size_kb = OUTPUT_HTML.stat().st_size / 1024
    print(f"✅ 빌드 완료: {OUTPUT_HTML}")
    print(f"   파일 크기: {size_kb:.0f}KB (JSON 인라인 포함)")
    print(f"   fetch() 제거 → 단일 파일로 로컬/Vercel 모두 동작")


if __name__ == "__main__":
    build()
