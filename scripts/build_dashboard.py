"""
dashboard.html → web/sales-dashboard.html 빌드 스크립트입니다.

dashboard.html이 Supabase Storage에서 데이터를 fetch하고
상단 툴바(돌아가기 링크 + 기간 선택기)도 직접 포함하므로
이 스크립트는 단순히 파일을 web/ 폴더로 복사만 합니다.
"""

from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DASHBOARD_HTML = SCRIPT_DIR / "output" / "dashboard.html"
OUTPUT_HTML = SCRIPT_DIR.parent / "web" / "sales-dashboard.html"


def build():
    html = DASHBOARD_HTML.read_text(encoding="utf-8")
    OUTPUT_HTML.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_HTML.write_text(html, encoding="utf-8")

    size_kb = OUTPUT_HTML.stat().st_size / 1024
    print(f"✅ 빌드 완료: {OUTPUT_HTML}")
    print(f"   파일 크기: {size_kb:.0f}KB")
    print(f"   데이터는 Supabase Storage에서 런타임 fetch합니다.")


if __name__ == "__main__":
    build()
