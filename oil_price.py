"""
每日油价速报：抓取国家发改委成品油调价公告，生成本轮调价日历、幅度、每升折算，
并按公告自述的计价规则预估下一轮调价的方向与幅度。

数据源（均为公开页面，无需密钥）：
  1. 国家发改委「新闻发布」栏目：https://www.ndrc.gov.cn/xwdt/xwfb/
     调价公告正文形如「自11月12日24时起，国内汽、柴油（标准品）价格每吨分别下调645元、625元」。
  2. 陕西省发改委（配合 --province shaanxi）「公示公告」中的调价通告与解读新闻：
     https://sndrc.shaanxi.gov.cn/sy/xwxx/gggg/
     该省新闻会直接给出 92#/0# 每升调幅与调价后价格，可用于校准本脚本的每升折算。
  3. 节假日接口 https://timor.tech/api/holiday/year/<年>，用于推算「10 个工作日」调价窗口，
     结果按年缓存到 .cache 目录，接口失败时退化为「周一至周五即为工作日」。
  4. 新浪财经外盘行情：hf_OIL / hf_CL 实时价仅作参考展示；
     OIL / CL 日 K 历史用于按「本周期均价 vs 上周期均价」预估下一轮调价。

用法：
  python tools/oil_price/oil_price.py              # 打印今日报告
  python tools/oil_price/oil_price.py --days 10    # 多出未来 10 天的窗口日历
  python tools/oil_price/oil_price.py --province shaanxi
  python tools/oil_price/oil_price.py --no-crude   # 跳过外盘行情
  python tools/oil_price/oil_price.py --notify     # 同时推送青龙通知（Markdown 正文）

青龙面板：脚本内容粘贴到「脚本管理」，定时规则建议 `0 17 * * *`（每天 17:00，
调价公告一般在 17:00 前后发布）；须勾选任务命令行末尾的 --notify 参数：
  task oil_price.py --notify --province shaanxi
通知走面板已经配好的渠道（QLAPI.systemNotify），无需额外配置；
面板外运行时若设置了 QINGLONG_WEBHOOK 环境变量，则改为向该 webhook 推送。
"""

from __future__ import annotations

import argparse
import builtins
import datetime as dt
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# 发改委「新闻发布」栏目首页（列表分页为 index_1.html、index_2.html …）
NDRC_LIST_URL = "https://www.ndrc.gov.cn/xwdt/xwfb/"
# 列表页每页约 25 条，翻页上限用于控制网络请求次数
NDRC_MAX_PAGES = 10
# 节假日数据按年缓存；优先读缓存，缺失时才请求接口
HOLIDAY_API = "https://timor.tech/api/holiday/year/%d"
# 陕西公示公告栏目，通告文件名形如 t20261112_3724352.html
SHAANXI_LIST_URL = "https://sndrc.shaanxi.gov.cn/sy/xwxx/gggg/"
# 陕西「委内动态」栏目：调价当天的解读新闻给出 92#/0# 每升调幅，是校准每升折算的官方口径
SHAANXI_NEWS_URL = "https://sndrc.shaanxi.gov.cn/sy/xwxx/wndt/"
# 陕西栏目每页按标签页各列 25 条，最近两页足以覆盖最近几次调价
SHAANXI_MAX_PAGES = 2
# 只需为最近几次调价核对官方每升口径，抓太多会拖慢单次运行
SHAANXI_RECENT = 3
# 外盘原油行情
SINA_QUOTE_URL = "https://hq.sinajs.cn/list=hf_OIL,hf_CL"
# 调价周期为「10 个工作日」
WINDOW_WORKDAYS = 10
# 每升折算系数（升/吨）：汽油 1388、柴油 1176，系发改委引用的通行口径
LITER_PER_TON = {"gas": 1388, "diesel": 1176}
# 每吨调价额折算到每升：元/吨 ÷ 升/吨；1 元/升 ≈ 汽油 20 元/吨、柴油 17 元/吨
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
    # 陕西站对新会话偶发 SSL 握手失败，关闭 keep-alive 可稳定复现成功
    "Connection": "close",
}


def log(msg: str) -> None:
    """输出进度信息到 stderr，避免污染 stdout 的报告正文。"""
    print(msg, file=sys.stderr)


def fetch(url: str, timeout: int = 25, retries: int = 2) -> str:
    """抓取网页并按 UTF-8 解码；站点均返回 UTF-8，个别页面声明错误也不影响。"""
    last_err = None
    for attempt in range(retries):
        try:
            # 每次新建 Session：复用连接时部分政府站点会重置 TLS
            with requests.Session() as s:
                s.headers.update(HEADERS)
                r = s.get(url, timeout=timeout)
                r.raise_for_status()
                return r.content.decode("utf-8", errors="replace")
        except Exception as exc:  # 网络抖动重试
            last_err = exc
            time.sleep(0.6 * (attempt + 1))
    raise RuntimeError("请求失败：%s（%s）" % (url, last_err))


def fetch_many(urls: list[str], workers: int = 6) -> dict[str, str]:
    """并发抓取，任一失败不影响其它；返回 {url: html}，失败的 url 不出现在结果里。"""
    out: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(fetch, u): u for u in urls}
        for fut in as_completed(futures):
            u = futures[fut]
            try:
                out[u] = fut.result()
            except Exception as exc:
                log("抓取失败：%s" % exc)
    return out


def html_to_lines(html: str) -> list[str]:
    """把 HTML 转为纯文本行，过滤脚本与样式块。"""
    text = re.sub(r"<script.*?</script>|<style.*?</style>", "", html, flags=re.S)
    text = re.sub(r"<[^>]+>", "\n", text)
    return [line.strip() for line in text.split("\n") if line.strip()]


def extract_article(html: str) -> str:
    """截取正文：发改委页面用 <!--startprint--> 标记，缺失时退回整页文本。"""
    m = re.search(r"<!--startprint-->(.*?)<!--endprint-->", html, re.S)
    return m.group(1) if m else html


# ---------------------------------------------------------------------------
# 调价公告解析
# ---------------------------------------------------------------------------

# 两种句式并存，必须都能命中：
#   普通公告：每吨分别下调645元、625元
#   调控公告：每吨分别应上调830元、800元，调控后实际上调395元、385元
# 故「每吨」可选、动词前允许「应/实际」等修饰词；抓到的每组按出现顺序排列，最后一组为实际执行值。
AMOUNT_RE = re.compile(
    r"(?:每吨)?(?:分别)?(?:调控后)?(?:应|实际|拟)?"
    r"(上调|下调|上涨|下跌|提高|降低|上升|下降)"
    r"(\d+(?:\.\d+)?)元(?:[、，,和](\d+(?:\.\d+)?)元)?"
)
# 列表条目形如：<li><a href="./202611/txxx.html" target="_blank" title="标题">标题</a><span>2026/11/12</span></li>
LIST_ITEM_RE = re.compile(
    r'<li>\s*<a href="([^"]+)"[^>]*title="([^"]*)"[^>]*>.*?</a>\s*<span>([^<]*)</span>', re.S
)
EFFECT_RE = re.compile(r"自(\d{1,2})月(\d{1,2})日24时起")
TITLE_DATE_RE = re.compile(r"(\d{4})年(\d{1,2})月(\d{1,2})日")


def parse_notice(title: str, url: str, html: str) -> dict:
    """解析一条调价公告。

    返回字段：
      date    公告日期（ISO 字符串，即调价当天，24 时才生效）
      effect  生效日（24 时生效，次日 0 点起可按新价加油，故通常等于 date）
      gas / diesel  汽油、柴油每吨调价额（元，正=上调，负=下调）
      cap_gas / cap_diesel  被调控压低的应调额（无调控时为 None）
      kind    up / down / hold / control（调控价）
      text    正文摘要（用于验证解析结果）
    """
    body = " ".join(html_to_lines(extract_article(html)))
    title_date = TITLE_DATE_RE.search(title)
    notice_date = (
        "%04d-%02d-%02d" % tuple(int(x) for x in title_date.groups()) if title_date else None
    )
    effect = EFFECT_RE.search(body)
    if effect and notice_date:
        # 正文「自X月Y日24时起」与标题日期一致时，生效时刻为 X 月 Y 日 24:00
        effect_at = "%s-%02d-%02d 24:00" % (notice_date[:4], int(effect.group(1)), int(effect.group(2)))
    elif notice_date:
        effect_at = notice_date + " 24:00"
    else:
        effect_at = None

    amounts = AMOUNT_RE.findall(body)
    cap_note = None
    gas = diesel = None
    kind = "hold"
    if amounts:
        # 调控公告会同时出现「应上调 X」「实际上调 Y」，取最后一段（实际值），
        # 并把前面的「应上调」记入 cap_note 供说明使用。
        sign = -1 if amounts[-1][0] in ("下调", "下跌", "降低", "下降") else 1
        gas = sign * float(amounts[-1][1])
        # 汽柴油同幅时公告只给一个数（如「每吨均上涨85元」），此时柴油等于汽油
        diesel = sign * float(amounts[-1][2]) if amounts[-1][2] else gas
        kind = "up" if sign > 0 else "down"
        if len(amounts) > 1:
            cap = amounts[0]
            cap_note = {
                "gas": float(cap[1]),
                "diesel": float(cap[2]) if cap[2] else None,
            }
            kind = "control"
    elif "不作调整" in title or "不作调整" in body:
        kind = "hold"

    return {
        "date": notice_date,
        "effect_at": effect_at,
        "gas": gas,
        "diesel": diesel,
        "cap": cap_note,
        "kind": kind,
        "title": title,
        "url": url,
        "text": body[:400],
    }


def collect_ndrc(year_from: int) -> list[dict]:
    """从发改委列表页收集 >= year_from 的成品油调价公告并逐条解析正文。"""
    prefix = NDRC_LIST_URL
    items = []
    for page in range(NDRC_MAX_PAGES):
        url = prefix if page == 0 else prefix + "index_%d.html" % page
        html = fetch(url)
        rows = LIST_ITEM_RE.findall(html)
        if not rows:
            break
        stop = False
        for href, title, date in rows:
            name = title.strip()
            y = int(date.strip()[:4]) if date.strip()[:4].isdigit() else 0
            if y < year_from:
                stop = True
                continue
            if "成品油" in name:
                items.append((name, NDRC_LIST_URL + href.lstrip("./")))
        if stop:
            break

    notices = []
    # 正文并发抓取，避免逐条串行导致数十秒等待
    fetched = fetch_many([link for _, link in items])
    for name, link in items:
        if link not in fetched:
            log("跳过公告（抓取失败）%s" % name)
            continue
        try:
            notices.append(parse_notice(name, link, fetched[link]))
        except Exception as exc:  # 单条公告失败不影响整体
            log("跳过公告 %s：%s" % (name, exc))
    # 同一日期可能同时存在「调控措施」通报与正式调价公告，合并时优先保留带调价额的
    merged: dict[str, dict] = {}
    for n in notices:
        if not n["date"]:
            continue  # 无日期的公告无法排序，直接丢弃
        key = n["date"]
        old = merged.get(key)
        if old is None or (old["gas"] is None and n["gas"] is not None):
            merged[key] = n
    return sorted(merged.values(), key=lambda x: x["date"])


def collect_shaanxi_realized(notices: list[dict]) -> dict[str, dict]:
    """抓取陕西省发改委的解读新闻，取官方给出的 92#/0# 每升调幅作为「已实现」校准值。

    陕西站只有「解读」类新闻会写每升调幅，且分散在两个栏目，因此同时扫：
      - 公示公告 SHAANXI_LIST_URL（文件通告，多数只给执行时间）
      - 委内动态 SHAANXI_NEWS_URL（记者解读稿，含「92号汽油……每升分别下调0.51元」）
    只处理最近 SHAANXI_RECENT 次公告：更早的历史调价无人关心，抓全部会让单次运行多花半分钟。
    返回 {公告日期: {"gas": 0.51, "diesel": 0.53, "src": url}}；抓不到时返回空 dict，
    渲染层会跳过该行，不影响主流程。
    """
    realized: dict[str, dict] = {}
    # 公告列表按日期倒序，末尾即最新；只核对最近几次（含最新不调价情形），更早的无需求价值
    wanted = {n["date"] for n in sorted(notices, key=lambda x: x["date"], reverse=True)[:SHAANXI_RECENT]}
    links: list[str] = []
    for prefix in (SHAANXI_LIST_URL, SHAANXI_NEWS_URL):
        # 只扫描本栏目自身文章：优先匹配带栏目前缀的绝对链接，也兼容相对链接；
        # 页脚还有政府信息公开等其它栏目的绝对链接，混进来会拉到无关页面（甚至 404），必须排除
        pattern = re.compile(
            r'href="(?:%s)?(\./?)?(\d{6}/t\d{8}_\d+\.html)"' % re.escape(prefix))
        # 通告与解读稿都发在调价当天，前两页足够覆盖最近几次调价
        for page in range(SHAANXI_MAX_PAGES):
            ul = prefix if page == 0 else prefix + "index_%d.html" % page
            try:
                html = fetch(ul)
            except Exception as exc:
                log("陕西栏目抓取失败：%s" % exc)
                break
            for dot, tail in pattern.findall(html):
                # 相对链接形如 ./202611/xxx.html，需补回栏目前缀
                full = tail if dot is None else prefix + tail
                if full not in links:
                    links.append(full)
    pages = fetch_many(links)
    for link in links:
        if link not in pages:
            continue
        body = " ".join(html_to_lines(pages[link]))
        m = re.search(r"92号汽油和0号柴油最高零售价格每升分别(上调|下调)([\d.]+)元和([\d.]+)元", body)
        if not m:
            continue
        d = re.search(r"自(\d{4})年(\d{1,2})月(\d{1,2})日24时起", body)
        if not d:
            continue
        key = "%s-%02d-%02d" % (d.group(1), int(d.group(2)), int(d.group(3)))
        if key in wanted and key not in realized:
            sign = -1 if m.group(1) == "下调" else 1
            realized[key] = {"gas": sign * float(m.group(2)), "diesel": sign * float(m.group(3)), "src": link}
    return realized


# ---------------------------------------------------------------------------
# 调价窗口日历：以工作日计数的 10 个工作日周期
# ---------------------------------------------------------------------------


def _cache_path(name: str) -> str:
    os.makedirs(CACHE_DIR, exist_ok=True)
    return os.path.join(CACHE_DIR, name)


def load_year_days(year: int) -> tuple[set, set]:
    """读取某年的假期与补班日；优先缓存，缺失时请求接口，失败则返回空集（退化为普通周末规则）。"""
    path = _cache_path("holiday_%d.json" % year)
    data = None
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f)
        except Exception:
            data = None
    # 当年（或未来年份）的放假安排通常上年 11 月才公布，此前接口只返回部分日期，需每隔几天重取
    stale = (dt.date.today() - dt.date.fromtimestamp(os.path.getmtime(path))).days > 7 if os.path.exists(path) else False
    if data is None or (year >= dt.date.today().year and stale):
        try:
            with requests.Session() as s:
                s.headers.update(HEADERS)
                r = s.get(HOLIDAY_API % year, timeout=15)
                r.raise_for_status()
                data = r.json()
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False)
        except Exception as exc:
            log("节假日接口不可用（%d 年），使用缓存或按周末规则推算：%s" % (year, exc))
            if data is None:
                return set(), set()
    off, work = set(), set()
    for key, val in (data.get("holiday") or {}).items():
        try:
            day = dt.date(year, int(key[:2]), int(key[3:]))
        except ValueError:
            continue
        (off if val.get("holiday") else work).add(day)
    return off, work


class WorkCalendar:
    """工作日判定：补班日算工作日，法定假日不算；其余按周一至周五。"""

    def __init__(self) -> None:
        self._years: dict[int, tuple[set, set]] = {}

    def _days(self, year: int) -> tuple[set, set]:
        if year not in self._years:
            self._years[year] = load_year_days(year)
        return self._years[year]

    def is_workday(self, day: dt.date) -> bool:
        off, work = self._days(day.year)
        return day in work or (day.weekday() < 5 and day not in off)

    def window_end(self, last_change: dt.date) -> dt.date:
        """上次调价的第 10 个工作日后，即下一个调价日。"""
        day, n = last_change, 0
        while n < WINDOW_WORKDAYS:
            day += dt.timedelta(days=1)
            if self.is_workday(day):
                n += 1
        return day

    def list_workdays(self, start: dt.date, end: dt.date) -> list[dt.date]:
        """闭区间内的工作日列表。"""
        days, day = [], start
        while day <= end:
            if self.is_workday(day):
                days.append(day)
            day += dt.timedelta(days=1)
        return days


def build_cycle(notices: list[dict], cal: WorkCalendar, today: dt.date) -> dict:
    """定位当前周期：上一次已生效调价、下一个调价日、更远的窗口。"""
    last = None
    for n in notices:
        if not n["date"]:
            continue
        if dt.date.fromisoformat(n["date"]) <= today:
            last = n
    nxt = None
    future = []
    if last:
        d = dt.date.fromisoformat(last["date"])
        for _ in range(6):
            d = cal.window_end(d)
            future.append(d)
        # 下一次调价日：最后一个已调价日之后的第一个窗口
        nxt = future[0]
    return {"last": last, "next": nxt, "future": future}


# ---------------------------------------------------------------------------
# 外盘原油参考（仅展示）
# ---------------------------------------------------------------------------


def fetch_crude() -> list[dict]:
    """新浪外盘行情：hf_OIL 布伦特、hf_CL 纽约原油（美元/桶）。"""
    out = []
    try:
        with requests.Session() as s:
            s.headers.update({"User-Agent": HEADERS["User-Agent"], "Referer": "https://finance.sina.com.cn/"})
            r = s.get(SINA_QUOTE_URL, timeout=15)
            r.encoding = "gbk"
        for name, payload in re.findall(r'hq_str_(\w+)="([^"]*)"', r.text):
            f = payload.split(",")
            if len(f) < 14 or not f[0]:
                continue
            price = float(f[0])
            prev = float(f[7]) if f[7] else 0.0
            out.append(
                {
                    "ticker": name,
                    "label": f[13] if len(f) > 13 else name,
                    "price": price,
                    "prev": prev,
                    "change_pct": (price - prev) / prev * 100 if prev else 0.0,
                    "quote_date": f[12] if len(f) > 12 else "",
                }
            )
    except Exception as exc:
        log("外盘行情获取失败：%s" % exc)
    return out


# ---------------------------------------------------------------------------
# 下一轮调价预估
#
# 国家发改委的公告只在调价当天发布，不会提前给出下一轮的幅度。但公告正文自己写明了
# 计价依据：「本次调价的前10个工作日平均价格」与「上次调价前10个工作日平均价格」相比。
# 因此可以用外盘原油日线复现这个比较过程，得到方向与大致幅度。
#
# 系数用 2026 年已发生的调价回归得到（过原点最小二乘，排除临时调控日）：
#   n=16, r=0.989, 平均绝对误差 87 元/吨。
# 只能当作区间参考，不能当作定论：临时调控、汇率、税费变化都会让实际值偏离。
# ---------------------------------------------------------------------------

# 新浪外盘日 K：注意 symbol 必须是 OIL / CL，hf_OIL 之类的实时代码在此接口返回 null
SINA_KLINE_URL = (
    "https://stock2.finance.sina.com.cn/futures/api/jsonp.php/x/"
    "GlobalFuturesService.getGlobalFuturesDailyKLine?symbol=%s"
)
# 原油均价每变动 1%，国内成品油每吨约同向变动该值（据 2026 年 16 次非调控调价拟合）
CRUDE_TO_YUAN_PER_TON = 51.4
# 历史回归的平均绝对误差，用于给出区间而不是单点
FORECAST_MAE = 87
# 调价机制规定幅度不足 50 元/吨时不调整
HOLD_THRESHOLD = 50


def fetch_crude_daily() -> dict[str, dict[str, float]]:
    """抓取布伦特（OIL）与纽约原油（CL）日 K 收盘价。

    返回 {"OIL": {"2026-11-16": 94.89, ...}, "CL": {...}}；任一失败则返回空 dict。
    """
    out: dict[str, dict[str, float]] = {}
    for ticker in ("OIL", "CL"):
        try:
            with requests.Session() as s:
                s.headers.update({"User-Agent": HEADERS["User-Agent"],
                                  "Referer": "https://finance.sina.com.cn/"})
                r = s.get(SINA_KLINE_URL % ticker, timeout=20)
            text = r.content.decode("utf-8", errors="replace")
            # 返回的是 jsonp：x([{"date":"2026-11-16","close":"94.890",...}, ...])
            m = re.search(r"x\((\[.*\])\)", text, re.S)
            if not m:
                log("%s 日 K 解析失败（接口可能变更）" % ticker)
                continue
            out[ticker] = {row["date"]: float(row["close"]) for row in json.loads(m.group(1))}
        except Exception as exc:
            log("%s 日 K 获取失败：%s" % (ticker, exc))
    return out


def _crude_index(daily: dict[str, dict[str, float]], start: dt.date, end: dt.date) -> tuple[float, int]:
    """区间内布伦特与纽约原油收盘价的等权均值，作为「一篮子原油」价格。

    只统计布伦特与纽约原油都有报价的日期，避免单边缺失造成偏移；
    返回 (均价, 有效天数)，无数据时返回 (0.0, 0)。
    """
    both = set(daily.get("OIL", {})) & set(daily.get("CL", {}))
    vals = [(daily["OIL"][d] + daily["CL"][d]) / 2
            for d in both if start <= dt.date.fromisoformat(d) <= end]
    return (sum(vals) / len(vals), len(vals)) if vals else (0.0, 0)


def forecast_next(notices: list[dict], cycle: dict, today: dt.date) -> dict | None:
    """按发改委公告自述的计价规则，预估下一次调价的方向与幅度。

    规则：本计价周期前 10 个工作日的原油均价，与上一周期（即上次调价所依据的那 10 个工作日）对比。
    上一周期用「上次调价公告日之前的 10 个工作日」近似，当前周期取「上次调价次日 ~ 今天」，
    因此当前周期未满 10 天时，结果只是阶段值，会随后续行情变化。

    返回 {pct, yuan, liters, days, total, kind} 或 None（数据不足时）。
    """
    nxt = cycle.get("next")
    if not nxt or nxt <= today:
        return None  # 窗口已到/已过，估算没有意义，交给公告说话
    daily = fetch_crude_daily()
    if not daily:
        return None
    last_date = dt.date.fromisoformat(cycle["last"]["date"]) if cycle.get("last") else None
    if last_date is None:
        return None

    # 上一周期：上次调价公告日之前（含当天）的 10 个工作日附近，取自然日区间近似
    prev_start = last_date - dt.timedelta(days=13)
    prev_avg, _ = _crude_index(daily, prev_start, last_date)
    # 当前周期：调价次日到最新一个已收盘日
    cur_start = last_date + dt.timedelta(days=1)
    cur_avg, days = _crude_index(daily, cur_start, today)
    if not prev_avg or not cur_avg:
        return None

    pct = (cur_avg / prev_avg - 1) * 100
    yuan = pct * CRUDE_TO_YUAN_PER_TON
    total = WorkCalendar().list_workdays(cur_start, nxt)
    return {
        "pct": pct,
        "yuan": yuan,
        "liters": (yuan / LITER_PER_TON["gas"], yuan / LITER_PER_TON["diesel"]),
        "days": days,
        "total": len(total) if total else WINDOW_WORKDAYS,
        "kind": "hold" if abs(yuan) < HOLD_THRESHOLD else ("up" if yuan > 0 else "down"),
    }


# ---------------------------------------------------------------------------
# 报告
# ---------------------------------------------------------------------------


def fmt_amount(v: float | None) -> str:
    if v is None:
        return "-"
    return "%+.0f 元/吨" % v


def fmt_liter(v: float | None) -> str:
    if v is None:
        return "-"
    return "%+.3f 元/升" % v


def liter_estimate(amount: float | None, kind: str) -> float | None:
    """每吨调价额折算为每升变动：元/升 = 元/吨 ÷ 升/吨。"""
    if amount is None:
        return None
    return amount / LITER_PER_TON[kind]


def effect_datetime(effect_at: str | None) -> dt.datetime | None:
    """把公告的「YYYY-MM-DD 24:00」转成真实生效时刻（次日 00:00）。"""
    if not effect_at:
        return None
    day, _, clock = effect_at.partition(" ")
    hour, _, minute = clock.partition(":")
    return dt.datetime.fromisoformat(day) + dt.timedelta(hours=int(hour), minutes=int(minute))


def render(notices: list[dict], cycle: dict, realized: dict, crude: list[dict], today: dt.date,
           days_ahead: int, forecast: dict | None = None) -> str:
    """生成报告正文。采用 Markdown，青龙通知（支持 MD 的渠道）可直接渲染成表格。"""
    now = dt.datetime.now()
    last = cycle["last"]
    nxt = cycle["next"]
    lines = []
    add = lines.append
    add("# 国内成品油调价速报（%s）" % now.strftime("%Y-%m-%d %H:%M"))

    # 顶部一句话摘要：先给结论，细节放后面
    summary = []
    if last:
        if last["kind"] == "hold":
            summary.append("最新一次（%s）不作调整" % last["date"][5:])
        else:
            summary.append("最新一次（%s）汽油 %s / 柴油 %s（约 %+.3f / %+.3f 元/升）" % (
                last["date"][5:], fmt_amount(last["gas"]), fmt_amount(last["diesel"]),
                liter_estimate(last["gas"], "gas"), liter_estimate(last["diesel"], "diesel")))
    if nxt:
        summary.append("下次窗口 %s" % nxt)
    if forecast:
        summary.append("预估%s %+.0f 元/吨" % (
            {"up": "上调", "down": "下调", "hold": "搁浅"}[forecast["kind"]], forecast["yuan"]))
    if summary:
        add("")
        add("> " + "；".join(summary))

    add("")
    add("## 最近一次调价")
    add("")
    if last:
        # 公告「24 时起执行」即次日零点生效；公告日当天下午发布，用生效时刻判断当前是否已执行
        eff = effect_datetime(last["effect_at"])
        if eff is None:
            when = "-"
        elif now >= eff:
            when = "已生效"
        elif eff.date() <= today:
            when = "今日 24 时生效"
        else:
            when = "待生效"
        add("- 公告日期：%s（%s）" % (last["date"], when))
        add("- 生效时间：%s" % (last["effect_at"] or "-"))
        add("- 方式：%s" % {"up": "上调", "down": "下调", "hold": "不作调整",
                            "control": "临时调控（限幅上调）"}[last["kind"]])
        if last["kind"] != "hold":
            add("- 每吨：汽油 %s，柴油 %s" % (fmt_amount(last["gas"]), fmt_amount(last["diesel"])))
            add("- 每升预估：汽油 **%s**，柴油 **%s**" % (
                fmt_liter(liter_estimate(last["gas"], "gas")),
                fmt_liter(liter_estimate(last["diesel"], "diesel"))))
            if last["cap"]:
                add("- 机制应上调 %s / %s，被临时调控压减" % (
                    fmt_amount(last["cap"]["gas"]), fmt_amount(last["cap"]["diesel"])))
        # 陕西官方每升口径：覆盖调价与不调价两类；不调价通常无校准值
        real = realized.get(last["date"])
        if real:
            add("- 陕西官方口径：92# **%+.2f 元/升**，0# **%+.2f 元/升**" % (real["gas"], real["diesel"]))
        add("- 原文：%s" % last["url"])
    else:
        add("- 未获取到调价公告，请检查网络或栏目地址是否变化。")

    add("")
    if nxt:
        delta = (nxt - today).days
        if delta < 0:
            add("## 下一个调价窗口：%s（已过窗口，公告可能未抓到，请核对发改委网站）" % nxt)
        elif delta == 0:
            add("## 下一个调价窗口：今天（%s）24 时，留意下午公告" % nxt)
        else:
            add("## 下一个调价窗口：**%s（还有 %d 天）**" % (nxt, delta))
        last_d = dt.date.fromisoformat(last["date"])
        wd = WorkCalendar().list_workdays(last_d + dt.timedelta(days=1), nxt)
        if wd:
            add("")
            add("本轮计价周期 %s ~ %s，共 %d 个工作日；窗口日以国家发改委公告为准。" % (
                wd[0].isoformat(), wd[-1].isoformat(), len(wd)))
    else:
        add("## 无法推算调价窗口（缺少可用的调价公告）")

    # 下一轮预估：方向 + 大致区间；不给单点假精确，并注明是阶段值
    if forecast:
        add("")
        add("## 下一轮预估：%s" % {"up": "倾向于上调", "down": "倾向于下调",
                                  "hold": "大概率搁浅（不足 50 元/吨）"}[forecast["kind"]])
        add("")
        lo, hi = forecast["yuan"] - FORECAST_MAE, forecast["yuan"] + FORECAST_MAE
        add("- 原油变化：本周期均价较上周期 **%+.2f%%**（布伦特+纽约原油等权，共 %d 个交易日）"
            % (forecast["pct"], forecast["days"]))
        add("- 预估每吨：**%+.0f 元/吨**（区间 %+.0f ~ %+.0f）" % (forecast["yuan"], lo, hi))
        add("- 预估每升：汽油 **%+.2f ~ %+.2f 元/升**，柴油 **%+.2f ~ %+.2f 元/升**" % (
            lo / LITER_PER_TON["gas"], hi / LITER_PER_TON["gas"],
            lo / LITER_PER_TON["diesel"], hi / LITER_PER_TON["diesel"]))
        add("- 进度：本周期已过 %d/%d 个工作日，**结果会随后续行情变化**" % (
            forecast["days"], forecast["total"]))

    add("")
    add("## 窗口日历（后续 %d 天）" % days_ahead)
    add("")
    add("| 日期 | 星期 | 类型 | 备注 |")
    add("| --- | --- | --- | --- |")
    cal = WorkCalendar()
    d = today
    for _ in range(days_ahead + 1):
        marks = []
        if d == nxt:
            marks.append("**调价窗口 24 时**")
        for f in cycle["future"][1:]:
            if d == f:
                marks.append("预定窗口")
        add("| %s | %s | %s | %s |" % (
            d.isoformat(), "一二三四五六日"[d.weekday()],
            "工作日" if cal.is_workday(d) else "休", "、".join(marks) or "-"))
        d += dt.timedelta(days=1)

    add("")
    add("## 年内调价历史（%s 起，共 %d 次）" % (notices[0]["date"] if notices else "-", len(notices)))
    add("")
    add("| 公告日 | 方向 | 汽油 | 柴油 | 每升折算（汽/柴） |")
    add("| --- | --- | --- | --- | --- |")
    for n in notices:
        direction = {"up": "上调", "down": "下调", "control": "调控上调", "hold": "不调"}[n["kind"]]
        if n["kind"] == "hold":
            real = realized.get(n["date"])
            est = "%+.2f / %+.2f" % (real["gas"], real["diesel"]) if real else "-"
        else:
            est = "%.3f / %.3f" % (liter_estimate(n["gas"], "gas"), liter_estimate(n["diesel"], "diesel"))
        add("| %s | %s | %s | %s | %s |" % (
            n["date"], direction, fmt_amount(n["gas"]), fmt_amount(n["diesel"]), est))

    if crude:
        add("")
        add("## 外盘原油（美元/桶，%s）" % crude[0]["quote_date"])
        add("")
        add("| 品种 | 最新价 | 涨跌 |")
        add("| --- | --- | --- |")
        for c in crude:
            add("| %s | %.2f | %+.2f%% |" % (c["label"], c["price"], c["change_pct"]))

    add("")
    add("---")
    add("每升折算 = 每吨调价额 ÷ 升/吨（汽油 1388、柴油 1176），与各省实际执行偏差约 ±0.02 元/升。"
        "附表为 89# 汽油与 0# 柴油标准品吨价，92#/95# 各省在此基础上加价；实际零售价以加油站为准。")
    return "\n".join(lines)


def notify(title: str, content: str) -> str:
    """推送通知。

    优先使用青龙面板注入的内置对象 QLAPI（仅在面板任务执行器中可用），
    走面板「通知设置」已配置的渠道；命令行运行时该变量不存在，
    退化为用 QINGLONG_WEBHOOK 环境变量指定的 webhook（兼容 Server酱等 URL 推送）。
    返回一句结果说明，写进日志便于排错。
    """
    # 该名称由青龙执行器注入（面板内置变量，无需 import），普通解释器里不存在，
    # 因此不能直接写 QLAPI 否则会 NameError，只能按名字安全地查找
    qlapi = globals().get("QLAPI") or getattr(builtins, "QLAPI", None)
    if qlapi is not None:
        try:
            resp = qlapi.systemNotify({"title": title, "content": content})
            code = resp.get("code") if isinstance(resp, dict) else None
            if code == 200:
                return "已通过青龙通知推送"
            return "青龙通知返回异常：%s" % resp
        except Exception as exc:
            return "青龙通知调用失败：%r" % exc

    webhook = os.environ.get("QINGLONG_WEBHOOK", "").strip()
    if webhook:
        try:
            # 表单 POST 兼容 Server酱 / 自定义 webhook 脚本
            r = requests.post(webhook, data={"title": title, "desp": content, "content": content,
                                             "text": title + "\n" + content}, timeout=20)
            return "已通过 QINGLONG_WEBHOOK 推送（HTTP %d）" % r.status_code
        except Exception as exc:
            return "webhook 推送失败：%r" % exc
    return "未检测到青龙环境，跳过通知（仅打印到日志）"


def main() -> int:
    parser = argparse.ArgumentParser(description="每日国内油价调价速报")
    parser.add_argument("--days", type=int, default=7, help="日历预览天数（默认 7）")
    parser.add_argument("--year-from", type=int, default=None, help="历史起始年份（默认当年）")
    parser.add_argument("--province", choices=["shaanxi"], default=None,
                        help="额外抓取省级发改委官方每升口径，目前支持 shaanxi")
    parser.add_argument("--no-crude", action="store_true", help="不抓取外盘原油行情（同时跳过下一轮预估）")
    parser.add_argument("--notify", action="store_true",
                        help="输出后推送通知（青龙 QLAPI，或环境变量 QINGLONG_WEBHOOK）")
    parser.add_argument("--json", action="store_true", help="同时输出 JSON 以便二次处理")
    args = parser.parse_args()

    today = dt.date.today()
    year_from = args.year_from or today.year
    log("抓取发改委调价公告（%d 年起）…" % year_from)
    notices = collect_ndrc(year_from)
    if not notices:
        log("未解析到任何调价公告，请检查网络或页面结构。")
        return 2

    cal = WorkCalendar()
    cycle = build_cycle(notices, cal, today)
    realized = collect_shaanxi_realized(notices) if args.province == "shaanxi" else {}
    crude = [] if args.no_crude else fetch_crude()
    # 预估依赖外盘日线，--no-crude 视作「只看官方数据」，一并跳过
    forecast = None if args.no_crude else forecast_next(notices, cycle, today)

    report = render(notices, cycle, realized, crude, today, args.days, forecast)
    print(report)
    if args.json:
        print()
        print(json.dumps({"notices": notices, "cycle": {k: str(v) for k, v in cycle.items()},
                          "realized": realized, "crude": crude}, ensure_ascii=False, indent=2, default=str))

    if args.notify:
        # 标题带最关键信息，不调价时直接写明，省得点开正文
        title = "油价速报 %s" % today.strftime("%m-%d")
        if cycle["last"]:
            if cycle["last"]["kind"] == "hold":
                title += " 不作调整"
            else:
                title += " 汽油%+d元/吨" % cycle["last"]["gas"]
        log(notify(title, report))
    return 0


if __name__ == "__main__":
    sys.exit(main())
