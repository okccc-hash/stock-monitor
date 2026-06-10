#!/usr/bin/env python3
"""
每日股票监控脚本：检测价格/成交量异动，抓取新闻，通过 Webhook 推送简报。
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
import schedule
import yfinance as yf

# =============================================================================
# 全局配置 — 按需修改
# =============================================================================

WATCHLIST: list[str] = [
    "MP",
    "UAMY",
    "LAC",
    "NVDA",
    "TSM",
    "AVGO",
    "MRVL",
    "AXTI"
]

# Discord / 飞书 / 钉钉 Webhook URL（留空或占位符则仅打印到控制台）
WEBHOOK_URL: str = "https://discord.com/api/webhooks/1514355512052023519/g3OaIJXVoWxC0mqquk5PyWS8AkVNsvxXpvlYQD6eTtSfKyRH3NZQpS6Nl9zt8WquHMG0"

# 推送渠道: "discord" | "feishu" | "dingtalk"
WEBHOOK_TYPE: str = "discord"

# 价格异动阈值（涨跌幅绝对值，单位 %）
PRICE_CHANGE_THRESHOLD: float = 3.0

# 成交量异动倍数（相对过去 20 个交易日均量）
VOLUME_SPIKE_MULTIPLIER: float = 1.5

# 定时执行时间（本地时区，24 小时制）
SCHEDULE_TIME: str = "06:00"

# 每个标的最多展示的新闻条数
MAX_NEWS_PER_TICKER: int = 3

# 新闻回溯窗口（小时）
NEWS_LOOKBACK_HOURS: int = 24


def _load_dotenv() -> None:
    """从项目根目录 .env 加载配置（已在 .gitignore，不会进 GitHub）。"""
    env_path = Path(__file__).resolve().parent / ".env"
    if not env_path.is_file():
        return
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            key, value = key.strip(), value.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = value
    except OSError:
        pass


def apply_env_overrides() -> None:
    """允许 .env / GitHub Actions / shell 环境变量覆盖顶部配置。"""
    _load_dotenv()
    global WEBHOOK_URL, WEBHOOK_TYPE, WATCHLIST
    global PRICE_CHANGE_THRESHOLD, VOLUME_SPIKE_MULTIPLIER

    if url := os.environ.get("WEBHOOK_URL", "").strip():
        WEBHOOK_URL = url
    if hook_type := os.environ.get("WEBHOOK_TYPE", "").strip():
        WEBHOOK_TYPE = hook_type
    if watchlist_raw := os.environ.get("WATCHLIST", "").strip():
        try:
            parsed = json.loads(watchlist_raw)
            if isinstance(parsed, list) and parsed:
                WATCHLIST = [str(s).strip().upper() for s in parsed if str(s).strip()]
        except json.JSONDecodeError:
            WATCHLIST = [s.strip().upper() for s in watchlist_raw.split(",") if s.strip()]
    if threshold := os.environ.get("PRICE_CHANGE_THRESHOLD", "").strip():
        PRICE_CHANGE_THRESHOLD = float(threshold)
    if multiplier := os.environ.get("VOLUME_SPIKE_MULTIPLIER", "").strip():
        VOLUME_SPIKE_MULTIPLIER = float(multiplier)


# apply_env_overrides()

# =============================================================================
# 日志
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


# =============================================================================
# 数据抓取
# =============================================================================


def fetch_price_metrics(symbol: str) -> dict[str, Any] | None:
    """
    计算价格异动指标。
    对比「前一交易日收盘价」与「当前最新价」。
    """
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="1mo", auto_adjust=True)
        if hist is None or hist.empty or len(hist) < 2:
            logger.warning("[%s] 历史 K 线不足，跳过价格检测", symbol)
            return None

        prev_close = float(hist["Close"].iloc[-2])
        current_price = float(hist["Close"].iloc[-1])

        # 尝试获取更实时的报价
        try:
            fast = ticker.fast_info
            last = getattr(fast, "last_price", None) or getattr(fast, "lastPrice", None)
            if last and float(last) > 0:
                current_price = float(last)
        except Exception:
            pass

        if prev_close <= 0:
            return None

        change_pct = (current_price - prev_close) / prev_close * 100.0
        is_anomaly = abs(change_pct) >= PRICE_CHANGE_THRESHOLD

        return {
            "prev_close": prev_close,
            "current_price": current_price,
            "change_pct": change_pct,
            "is_anomaly": is_anomaly,
        }
    except Exception as exc:
        logger.error("[%s] 价格指标计算异常: %s", symbol, exc)
        return None


def fetch_volume_metrics(symbol: str) -> dict[str, Any] | None:
    """
    计算成交量异动指标。
    对比「前一交易日成交量」与「过去 20 个交易日均量」。
    """
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period="2mo", auto_adjust=True)
        if hist is None or hist.empty or len(hist) < 22:
            logger.warning("[%s] 历史 K 线不足 22 天，跳过量能检测", symbol)
            return None

        prev_volume = float(hist["Volume"].iloc[-2])
        avg_volume = float(hist["Volume"].iloc[-22:-2].mean())

        if avg_volume <= 0:
            return None

        ratio = prev_volume / avg_volume
        is_anomaly = ratio >= VOLUME_SPIKE_MULTIPLIER

        return {
            "prev_volume": prev_volume,
            "avg_volume_20d": avg_volume,
            "volume_ratio": ratio,
            "is_anomaly": is_anomaly,
        }
    except Exception as exc:
        logger.error("[%s] 成交量指标计算异常: %s", symbol, exc)
        return None


def _parse_news_timestamp(item: dict[str, Any]) -> datetime | None:
    """从 yfinance 新闻条目中解析发布时间。"""
    ts = item.get("providerPublishTime")
    if ts is not None:
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc)
        except (TypeError, ValueError, OSError):
            pass

    content = item.get("content") or {}
    pub_date = content.get("pubDate") or content.get("displayTime")
    if pub_date:
        for fmt in ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d %H:%M:%S"):
            try:
                dt = datetime.strptime(str(pub_date)[:19], fmt[:19].replace("T", " "))
                return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue
    return None


def _extract_news_fields(item: dict[str, Any]) -> tuple[str, str]:
    """提取标题与链接，兼容 yfinance 新旧数据结构。"""
    content = item.get("content") or {}
    title = (
        item.get("title")
        or content.get("title")
        or content.get("headline")
        or "（无标题）"
    )
    link = (
        item.get("link")
        or item.get("url")
        or content.get("canonicalUrl", {}).get("url")
        or content.get("clickThroughUrl", {}).get("url")
        or ""
    )
    return str(title).strip(), str(link).strip()


def fetch_news_yfinance(symbol: str) -> list[dict[str, str]]:
    """通过 yfinance 抓取近 24 小时新闻。"""
    results: list[dict[str, str]] = []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=NEWS_LOOKBACK_HOURS)

    try:
        ticker = yf.Ticker(symbol)
        raw_news = ticker.news or []
    except Exception as exc:
        logger.warning("[%s] yfinance 新闻获取失败: %s", symbol, exc)
        return results

    for item in raw_news:
        try:
            published = _parse_news_timestamp(item)
            if published and published < cutoff:
                continue
            title, link = _extract_news_fields(item)
            if not title or title == "（无标题）":
                continue
            results.append({"title": title, "link": link})
            if len(results) >= MAX_NEWS_PER_TICKER:
                break
        except Exception as exc:
            logger.debug("[%s] 解析单条新闻失败: %s", symbol, exc)
            continue

    return results


def fetch_news_pygooglenews(symbol: str) -> list[dict[str, str]]:
    """yfinance 无结果时，使用 pygooglenews 作为备用来源。"""
    results: list[dict[str, str]] = []
    try:
        from pygooglenews import GoogleNews

        gn = GoogleNews(lang="en", country="US")
        search = gn.search(f"{symbol} stock", when="1d")
        entries = search.get("entries", []) if search else []
    except Exception as exc:
        logger.warning("[%s] pygooglenews 获取失败: %s", symbol, exc)
        return results

    for entry in entries[: MAX_NEWS_PER_TICKER * 2]:
        try:
            title = entry.get("title", "").strip()
            link = entry.get("link", "").strip()
            if title:
                results.append({"title": title, "link": link})
            if len(results) >= MAX_NEWS_PER_TICKER:
                break
        except Exception:
            continue

    return results


def fetch_news(symbol: str) -> list[dict[str, str]]:
    """抓取新闻，优先 yfinance，不足时 fallback 到 pygooglenews。"""
    news = fetch_news_yfinance(symbol)
    if len(news) < MAX_NEWS_PER_TICKER:
        existing_titles = {n["title"] for n in news}
        for item in fetch_news_pygooglenews(symbol):
            if item["title"] not in existing_titles:
                news.append(item)
                existing_titles.add(item["title"])
            if len(news) >= MAX_NEWS_PER_TICKER:
                break
    return news[:MAX_NEWS_PER_TICKER]


# =============================================================================
# 简报格式化（A 股习惯：涨红跌绿）
# =============================================================================


def _format_change(change_pct: float) -> str:
    """格式化涨跌幅，带颜色 emoji 标记。"""
    if change_pct > 0:
        return f"🔴 **+{change_pct:.2f}%**"
    if change_pct < 0:
        return f"🟢 **{change_pct:.2f}%**"
    return f"⚪ **{change_pct:.2f}%**"


def _format_volume_ratio(ratio: float, is_anomaly: bool) -> str:
    flag = " ⚡量能异动" if is_anomaly else ""
    return f"{ratio:.2f}x 均量{flag}"


def build_report(results: list[dict[str, Any]]) -> str:
    """将所有标的监控结果聚合为 Markdown 简报。"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
    lines = [
        f"## 📊 股票监控简报",
        f"**生成时间:** {now_str}",
        "",
    ]

    anomaly_count = sum(
        1
        for r in results
        if r.get("price", {}).get("is_anomaly")
        or r.get("volume", {}).get("is_anomaly")
        or r.get("news")
    )

    if anomaly_count == 0:
        lines.append("> 今日监控列表暂无明显异动或新新闻。")
        lines.append("")

    for r in results:
        symbol = r["symbol"]
        price = r.get("price")
        volume = r.get("volume")
        news = r.get("news") or []

        has_price_alert = price and price.get("is_anomaly")
        has_volume_alert = volume and volume.get("is_anomaly")
        has_news = len(news) > 0

        if not (has_price_alert or has_volume_alert or has_news):
            continue

        tags: list[str] = []
        if has_price_alert:
            tags.append("价格异动")
        if has_volume_alert:
            tags.append("量能异动")
        if has_news:
            tags.append("新闻")

        lines.append(f"### {symbol} {' | '.join(f'[{t}]' for t in tags)}")
        lines.append("")

        if price:
            change_str = _format_change(price["change_pct"])
            alert = " ⚡**价格异动**" if price["is_anomaly"] else ""
            lines.append(
                f"- **价格:** {change_str}{alert} "
                f"（昨收 ${price['prev_close']:.2f} → 现价 ${price['current_price']:.2f}）"
            )

        if volume:
            vol_str = _format_volume_ratio(volume["volume_ratio"], volume["is_anomaly"])
            lines.append(
                f"- **成交量:** {vol_str} "
                f"（昨量 {volume['prev_volume']:,.0f} / 20日均 {volume['avg_volume_20d']:,.0f}）"
            )

        if news:
            lines.append("- **最新新闻:**")
            for i, item in enumerate(news, 1):
                title = item["title"]
                link = item.get("link", "")
                if link:
                    lines.append(f"  {i}. [{title}]({link})")
                else:
                    lines.append(f"  {i}. {title}")

        lines.append("")

    # 附录：无异动标的简要一览
    quiet = [
        r["symbol"]
        for r in results
        if not (
            (r.get("price") or {}).get("is_anomaly")
            or (r.get("volume") or {}).get("is_anomaly")
            or r.get("news")
        )
        and r.get("price")
    ]
    if quiet:
        lines.append("---")
        lines.append(f"**平稳标的:** {', '.join(quiet)}")

    return "\n".join(lines)


# =============================================================================
# Webhook 推送
# =============================================================================

DISCORD_CONTENT_LIMIT = 2000
DISCORD_EMBED_DESC_LIMIT = 4096
WEBHOOK_PLACEHOLDERS = ("YOUR_TOKEN_HERE", "YOUR_WEBHOOK_ID", "YOUR_WEBHOOK_TOKEN")


def _webhook_configured() -> bool:
    if not WEBHOOK_URL:
        return False
    return not any(marker in WEBHOOK_URL for marker in WEBHOOK_PLACEHOLDERS)


def _format_for_discord(text: str) -> str:
    """将通用 Markdown 转为 Discord 可读的格式。"""
    text = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"**\1**\n\2", text)
    text = re.sub(r"^### (.+)$", r"**\1**", text, flags=re.MULTILINE)
    text = re.sub(r"^## (.+)$", r"__\1__", text, flags=re.MULTILINE)
    return text.strip()


def _split_discord_chunks(text: str, limit: int) -> list[str]:
    chunks: list[str] = []
    remaining = text
    while remaining:
        if len(remaining) <= limit:
            chunks.append(remaining)
            break
        split_at = remaining.rfind("\n", 0, limit)
        if split_at <= 0:
            split_at = limit
        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].strip()
    return [c for c in chunks if c]


def _build_webhook_payload(markdown_text: str) -> list[dict[str, Any]]:
    """按渠道构造 Webhook 请求体，Discord 超长时拆成多条。"""
    hook_type = WEBHOOK_TYPE.lower()

    if hook_type == "dingtalk":
        return [{
            "msgtype": "markdown",
            "markdown": {"title": "股票监控简报", "text": markdown_text},
        }]

    if hook_type == "feishu":
        return [{
            "msg_type": "text",
            "content": {"text": markdown_text},
        }]

    # Discord
    formatted = _format_for_discord(markdown_text)
    if len(formatted) <= DISCORD_EMBED_DESC_LIMIT:
        return [{
            "embeds": [{
                "title": "📊 股票监控简报",
                "description": formatted,
                "color": 0x5865F2,
            }],
        }]

    return [{"content": chunk} for chunk in _split_discord_chunks(formatted, DISCORD_CONTENT_LIMIT)]


def _webhook_send_ok(resp: requests.Response) -> bool:
    hook_type = WEBHOOK_TYPE.lower()

    if hook_type == "discord":
        return resp.status_code in (200, 204)

    try:
        body = resp.json()
    except ValueError:
        return resp.ok

    if hook_type == "dingtalk":
        return body.get("errcode", 0) == 0

    code = body.get("code") or body.get("StatusCode")
    return code in (0, None, 200)


def send_webhook(markdown_text: str) -> bool:
    """
    将 Markdown 简报发送到 Discord / 飞书 / 钉钉 Webhook。
    返回 True 表示全部推送成功。
    """
    if not _webhook_configured():
        logger.info("Webhook URL 未配置，跳过推送。简报内容如下：\n%s", markdown_text)
        return False

    payloads = _build_webhook_payload(markdown_text)

    try:
        for index, payload in enumerate(payloads, start=1):
            resp = requests.post(
                WEBHOOK_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=30,
            )
            resp.raise_for_status()
            if not _webhook_send_ok(resp):
                logger.error("Webhook 推送失败 (第 %d 段): %s", index, resp.text[:500])
                return False

        logger.info("Webhook 推送成功（共 %d 段）", len(payloads))
        return True

    except requests.RequestException as exc:
        logger.error("Webhook 请求异常: %s", exc)
        return False
    except Exception as exc:
        logger.error("Webhook 推送未知异常: %s", exc)
        return False


# =============================================================================
# 主监控流程
# =============================================================================


def analyze_ticker(symbol: str) -> dict[str, Any]:
    """分析单个标的，单项失败不影响其他项。"""
    symbol = symbol.strip().upper()
    result: dict[str, Any] = {"symbol": symbol, "price": None, "volume": None, "news": []}

    try:
        result["price"] = fetch_price_metrics(symbol)
    except Exception as exc:
        logger.error("[%s] 价格分析顶层异常: %s", symbol, exc)

    try:
        result["volume"] = fetch_volume_metrics(symbol)
    except Exception as exc:
        logger.error("[%s] 成交量分析顶层异常: %s", symbol, exc)

    try:
        result["news"] = fetch_news(symbol)
    except Exception as exc:
        logger.error("[%s] 新闻抓取顶层异常: %s", symbol, exc)

    return result


def run_monitor() -> None:
    """执行一次完整监控并推送简报。"""
    logger.info("=" * 50)
    logger.info("开始执行股票监控，标的: %s", WATCHLIST)

    results: list[dict[str, Any]] = []
    for symbol in WATCHLIST:
        try:
            results.append(analyze_ticker(symbol))
        except Exception as exc:
            logger.error("[%s] 整体分析失败，已跳过: %s", symbol, exc)
            results.append({"symbol": symbol, "price": None, "volume": None, "news": []})

    try:
        report = build_report(results)
    except Exception as exc:
        logger.error("简报生成失败: %s", exc)
        report = f"## 股票监控简报\n\n简报生成异常: {exc}"

    try:
        sent = send_webhook(report)
        if not sent and _webhook_configured():
            if os.environ.get("GITHUB_ACTIONS") == "true":
                sys.exit(1)
    except Exception as exc:
        logger.error("推送环节异常: %s", exc)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            sys.exit(1)

    logger.info("本次监控完成")


def main() -> None:
    """入口：支持 --now 立即运行，否则按 SCHEDULE_TIME 定时调度。"""
    apply_env_overrides()

    if len(sys.argv) > 1 and sys.argv[1] in ("--now", "-n"):
        run_monitor()
        return

    logger.info("股票监控已启动，每日 %s 自动执行（Ctrl+C 退出）", SCHEDULE_TIME)
    schedule.every().day.at(SCHEDULE_TIME).do(run_monitor)

    while True:
        try:
            schedule.run_pending()
            time.sleep(30)
        except KeyboardInterrupt:
            logger.info("收到退出信号，监控停止")
            break
        except Exception as exc:
            logger.error("调度循环异常（将继续运行）: %s", exc)
            time.sleep(60)


if __name__ == "__main__":
    main()
