"""
dashboard.html → web/sales-dashboard.html 빌드 스크립트입니다.

dashboard.html이 Supabase Storage에서 데이터를 fetch하므로
JSON 인라인 없이 그대로 복사 + 네비게이션 링크만 추가합니다.
"""

from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DASHBOARD_HTML = SCRIPT_DIR / "output" / "dashboard.html"
OUTPUT_HTML = SCRIPT_DIR.parent / "web" / "sales-dashboard.html"


def build():
    # 1. 원본 HTML 로드
    html = DASHBOARD_HTML.read_text(encoding="utf-8")

    # 2. 메인 대시보드로 돌아가는 링크 추가 (헤더 아래)
    back_link = '<div style="background:#16213e;padding:10px 32px;"><a href="index.html" style="display:inline-flex;align-items:center;gap:8px;padding:6px 16px;background:rgba(59,130,246,0.15);border:1px solid rgba(59,130,246,0.3);color:#60a5fa;border-radius:6px;font-size:13px;font-weight:600;text-decoration:none;">📊 경영 대시보드로 돌아가기</a></div>'
    html = html.replace('</header>', f'</header>\n{back_link}')

    # 3. 저장
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")

    size_kb = OUTPUT_HTML.stat().st_size / 1024
    print(f"✅ 빌드 완료: {OUTPUT_HTML}")
    print(f"   파일 크기: {size_kb:.0f}KB")
    print(f"   데이터는 Supabase Storage에서 런타임 fetch합니다.")


if __name__ == "__main__":
    build()
