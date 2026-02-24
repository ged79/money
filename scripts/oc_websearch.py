"""OpenClaw용 무료 웹 검색 - DuckDuckGo (API 키 불필요)"""
import sys, os, json, argparse

if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

from duckduckgo_search import DDGS


def search(query: str, max_results: int = 5, region: str = "wt-wt", news: bool = False):
    results = []
    with DDGS() as ddgs:
        if news:
            raw = list(ddgs.news(query, max_results=max_results, region=region))
            for r in raw:
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("url", ""),
                    "snippet": r.get("body", ""),
                    "date": r.get("date", ""),
                    "source": r.get("source", ""),
                })
        else:
            raw = list(ddgs.text(query, max_results=max_results, region=region))
            for r in raw:
                results.append({
                    "title": r.get("title", ""),
                    "url": r.get("href", ""),
                    "snippet": r.get("body", ""),
                })
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--query", "-q", required=True, help="검색어")
    parser.add_argument("--max", "-m", type=int, default=5, help="결과 수 (기본 5)")
    parser.add_argument("--region", "-r", default="wt-wt", help="지역 (kr-kr, en-us 등)")
    parser.add_argument("--news", "-n", action="store_true", help="뉴스 검색")
    args = parser.parse_args()

    data = search(args.query, args.max, args.region, args.news)
    print(json.dumps(data, ensure_ascii=False, indent=2))
