"""
任务名称
name: 国内油价速报
定时规则
cron: 0 17 * * *

每日油价速报：抓取国家发改委成品油调价公告，生成本轮调价日历、幅度、每升折算，
并按公告自述的计价规则预估下一轮调价的方向与幅度。

上面的 name / cron 是青龙面板「脚本管理」识别的任务元信息（与官方示例同构）：
把本文件上传到脚本管理后，定时任务的命令填 `task oil_price.py --notify` 即可，
任务名与定时规则会从文件头自动带出。通知走 QLAPI.systemNotify，用面板已配置的渠道。

数据源（均为公开页面，无需密钥）：
  1. 国家发改委「新闻发布」栏目：https://www.ndrc.gov.cn/xwdt/xwfb/
     调价公告正文形如「自11月12日24时起，国内汽、柴油（标准品）价格每吨分别下调645元、625元」。
  2. 省级口径（配合 --province 二选一，不传则不显示任何省级数据）：
     shaanxi   陕西省发改委「公示公告」的调价通告与解读新闻：
               https://sndrc.shaanxi.gov.cn/sy/xwxx/gggg/
               该省新闻直接给出 92#/0# 每升调幅，可用于校准每升折算。
     guangdong 广东省发改委「业务公告」：https://drc.gd.gov.cn/ywgg/
               正文带 HTML 表格，直接给出 92#/0# 的零售吨价与每升价，可逐期核对。
  3. 节假日接口 https://timor.tech/api/holiday/year/<年>，用于推算「10 个工作日」调价窗口，
     结果按年缓存到 .cache 目录，接口失败时退化为「周一至周五即为工作日」。
  4. 新浪财经外盘行情：hf_OIL / hf_CL 实时价仅作参考展示；
     OIL / CL 日 K 历史用于按「本周期均价 vs 上周期均价」预估下一轮调价。

用法：
  python tools/oil_price/oil_price.py              # 打印今日报告
  python tools/oil_price/oil_price.py --days 10    # 多出未来 10 天的窗口日历
  python tools/oil_price/oil_price.py --province shaanxi   # 附带陕西官方每升调幅
  python tools/oil_price/oil_price.py --province guangdong # 附带广东官方每升价
  python tools/oil_price/oil_price.py --no-crude   # 跳过外盘行情
  python tools/oil_price/oil_price.py --notify     # 同时推送青龙通知（正文默认转成 HTML）

青龙面板里运行时会自动带上 --notify（见文件末尾 __main__ 分支）；面板外运行时若设置了
QINGLONG_WEBHOOK 环境变量，则改为向该 webhook 推送，否则只打印不通知。
"""

from __future__ import annotations

import argparse
import builtins
import datetime as dt
import html as html_lib
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
# 广东省发改委「业务公告」栏目：公告正文带 HTML 表格，直接给出 92#/0# 的官方每升价，
# 是本脚本每升折算系数的校准来源，也是报告里可用于交叉核对的官方口径
GD_LIST_URL = "https://drc.gd.gov.cn/ywgg/"
# 该栏目每页约 12 条，最近两次调价在第 1 页，取 1 页即可
GD_MAX_PAGES = 1
# 外盘原油行情
SINA_QUOTE_URL = "https://hq.sinajs.cn/list=hf_OIL,hf_CL"
# 调价周期为「10 个工作日」，出自《石油价格管理办法》（2016 年修订）第七条：
# 「汽、柴油价格根据国际市场原油价格变化每 10 个工作日调整一次。调价生效时间为调价发布日 24 时。」
WINDOW_WORKDAYS = 10
# 各标号的换算系数。公告只给「89# 汽油 / 0# 柴油」标准品的吨幅，92#/95# 零售价按品质比率同比例放大。
# 这一分层是《石油价格管理办法》明文规定的：
#   第十三条「本办法第五条至第十二条汽油、柴油价格均指标准品价格。国家发展改革委制定成品油标准品与
#   非标准品的品质比率。非标准品价格按照标准品价格和规定的品质比率确定。」
#   第十四条「省级价格主管部门会同有关部门制定本省（自治区、直辖市）汽、柴油吨与升折算的系数。」
# 即：吨幅只对标准品生效，92#/95# 要乘品质比率；吨→升 没有全国统一系数，由各省自定。
# 系数来自广东省发改委公告附表的实测反推（2026 年 20 期，吨价与每升价两列都有）：
#   品质比率（该标号吨价变化 ÷ 公告吨幅）：89# 1.0000、92# 1.0602、95# 1.1200、0# 1.0000（柴油恰好为 1）
#   升/吨（零售吨价 ÷ 零售每升价）：89# 1344.4、92# 1322.9、95# 1290.4、0# 1166.7
# 旧版误用 1388/1176 升/吨（实验室密度口径），导致汽油每升调幅系统性偏小约 14%。
GRADE_RATIO = {"89": 1.0, "92": 1.0602, "95": 1.1200, "0": 1.0}
GRADE_LITER_PER_TON = {"89": 1344.4, "92": 1322.9, "95": 1290.4, "0": 1166.7}
# 报告里展示的标号（92#/95# 汽油与 0# 柴油是私家车最关心的三档）
REPORT_GRADES = ("92", "95", "0")
# 保留旧常量名以便既有调用处（外盘/预估）继续按「汽油、柴油」两档工作
LITER_PER_TON = {"gas": GRADE_LITER_PER_TON["92"], "diesel": GRADE_LITER_PER_TON["0"]}
GAS_QUALITY_RATIO = GRADE_RATIO["92"]
CACHE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".cache")
# 通知渠道类型（QLAPI notificationInfo.type）。取值必须与青龙面板「通知设置」里已启用的渠道
# 完全一致，且**大小写敏感**：官方枚举为 pushPlus、telegramBot、dingtalkBot、weWorkBot、
# weWorkApp、feishu、bark、serverChan、pushDeer、gotify、goCqHttpBot、iGot、email、pushMe、
# webhook、chronocat、ntfy、wxPusherBot、wxPusherSpt、wpush、aibotk、wePlusBot、chat。
# 写成小写 pushplus 会让青龙返回 code=400「通知发送失败，请检查系统设置/通知配置」。
# 把环境变量显式设为空字符串（NOTIFY_TYPE=）则完全不传 notificationInfo，一切按面板
# 「通知设置」里的渠道与模板走——面板的 pushplus 模板键名是 pushplusTemplate 而非
# template，传 notificationInfo 反而可能把面板配置盖掉，排查渲染问题时优先用这种模式。
# 一般不用设置：留空表示不传 notificationInfo，渠道与模板完全按面板「通知设置」走。
# 只有确实需要强制指定渠道时才设置，且大小写必须与官方枚举一致（如 pushPlus、telegramBot）。
# 注意传了 notificationInfo 会整体替换面板配置，缺 token 时青龙返回 code=400。
NOTIFY_TYPE = os.environ.get("NOTIFY_TYPE", "").strip()
# 推送正文格式。青龙内置的 pushplus 渠道实际按 html 模板投递（面板里的 pushplusTemplate
# 字段没有生效：实测直接把 Markdown 发过去，手机端显示成一堆 # 和 | 的源码），
# 因此默认先把报告转成 HTML 再发；换成 Telegram、飞书等原生支持 Markdown 的渠道时，
# 把本变量设为 markdown 即可原样发送。
NOTIFY_TEMPLATE = os.environ.get("NOTIFY_TEMPLATE", "html").strip().lower()

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


def collect_guangdong_prices(notices: list[dict]) -> dict[str, dict]:
    """抓取广东省发改委的调价公告，取官方 92#/0# 每升价作为交叉核对口径。

    广东公告正文带 HTML 表格（不是图片），每行形如「92号汽油（Ⅵ） | 批发吨价 | 零售吨价 | 零售每升价」，
    因此可直接读出各省都要执行的官方每升价。相比陕西解读稿只在个别期出现，
    广东每次调价都有，用它核对每升折算最可靠，也是 GAS_QUALITY_RATIO / LITER_PER_TON 的来源。
    返回 {公告日期: {"gas_ton":.., "gas_liter":.., "diesel_ton":.., "diesel_liter":.., "src": url}}；
    抓不到时返回空 dict，渲染层会跳过该行。
    """
    prices: dict[str, dict] = {}
    # 最近两次调价的公告都在第 1 页；多翻页只会拖慢运行
    links: list[str] = []
    for page in range(GD_MAX_PAGES):
        ul = GD_LIST_URL if page == 0 else GD_LIST_URL + "index_%d.html" % page
        try:
            html = fetch(ul)
        except Exception as exc:
            log("广东栏目抓取失败：%s" % exc)
            break
        for href, title in re.findall(r'href="([^"]*post_\d+\.html)"[^>]*>\s*([^<]*成品油[^<]*)<', html):
            if "成品油价格" in title and href not in links:
                links.append(href if href.startswith("http") else GD_LIST_URL + href.lstrip("./"))
    if not links:
        return prices
    pages = fetch_many(links)
    # 只保留最近两次公告对应的日期，避免把全年表格都塞进报告
    wanted = {n["date"] for n in sorted(notices, key=lambda x: x["date"], reverse=True)[:2]}
    for link in links:
        if link not in pages:
            continue
        html = pages[link]
        d = re.search(r"自(\d{4})年(\d{1,2})月(\d{1,2})日24时起", re.sub(r"<[^>]+>", "", html))
        if not d:
            continue
        key = "%s-%02d-%02d" % (d.group(1), int(d.group(2)), int(d.group(3)))
        if key not in wanted or key in prices:
            continue
        # 表格按行拆成纯文本单元格，「|」分隔；最后一个是每升价
        rows: dict[str, list[str]] = {}
        for row in re.findall(r"<tr.*?</tr>", html, re.S):
            cells = [re.sub(r"\s+", "", re.sub(r"<[^>]+>", "", c))
                     for c in re.findall(r"<td.*?</td>", row, re.S)]
            cells = [c for c in cells if c]
            for name, tag in (("92", "92号汽油"), ("0", "0号柴油")):
                if cells and cells[0].startswith(tag):
                    rows[name] = cells
        if "92" in rows and "0" in rows:
            prices[key] = {
                "gas_ton": float(rows["92"][-2]), "gas_liter": float(rows["92"][-1]),
                "diesel_ton": float(rows["0"][-2]), "diesel_liter": float(rows["0"][-1]),
                "src": link,
            }
    return prices


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
# 《石油价格管理办法》第七条：「当调价幅度低于每吨 50 元时，不作调整，纳入下次调价时累加或冲抵。」
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
        "liters": (liter_estimate(yuan, "gas"), liter_estimate(yuan, "diesel")),
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
    """每吨调价额折算为每升变动。

    元/升 = 元/吨 × 品质比率 ÷ 升/吨。公告口径是 89# 汽油与 0# 柴油标准品，
    92#/95# 零售价按各自品质比率同比例放大，柴油比率恰好为 1。
    kind 传 "gas"/"diesel" 时按 92#/0# 处理，兼容早期只算两档的调用处。
    """
    if amount is None:
        return None
    grade = {"gas": "92", "diesel": "0"}.get(kind, kind)
    return amount * GRADE_RATIO[grade] / GRADE_LITER_PER_TON[grade]


def effect_datetime(effect_at: str | None) -> dt.datetime | None:
    """把公告的「YYYY-MM-DD 24:00」转成真实生效时刻（次日 00:00）。"""
    if not effect_at:
        return None
    day, _, clock = effect_at.partition(" ")
    hour, _, minute = clock.partition(":")
    return dt.datetime.fromisoformat(day) + dt.timedelta(hours=int(hour), minutes=int(minute))


def render(notices: list[dict], cycle: dict, realized: dict, crude: list[dict], today: dt.date,
           days_ahead: int, forecast: dict | None = None,
           official_prices: dict | None = None) -> str:
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
            summary.append("最新一次（%s）汽油 %s / 柴油 %s（92# 约 %+.2f、0# 约 %+.2f 元/升）" % (
                last["date"][5:], fmt_amount(last["gas"]), fmt_amount(last["diesel"]),
                liter_estimate(last["gas"], "92"), liter_estimate(last["diesel"], "0")))
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
            # 公告口径是 89# 汽油，92#/95# 按各自品质比率放大后折算每升，私家车最关心这三档
            add("- 每升预估：92# 汽油 **%s**，95# 汽油 **%s**，0# 柴油 **%s**" % (
                fmt_liter(liter_estimate(last["gas"], "92")),
                fmt_liter(liter_estimate(last["gas"], "95")),
                fmt_liter(liter_estimate(last["diesel"], "0"))))
            if last["cap"]:
                add("- 机制应上调 %s / %s，被临时调控压减" % (
                    fmt_amount(last["cap"]["gas"]), fmt_amount(last["cap"]["diesel"])))
        # 广东官方每升口径：公告附表的零售每升价，每次调价都有，是最可靠的可核对基准
        gd = (official_prices or {}).get(last["date"])
        prev_key = None
        if gd:
            # 调价前后每升价的差额即本次官方每升调幅；上期表格取本年较早的一条公告
            older = [n["date"] for n in notices if n["date"] < last["date"]]
            prev_key = older[-1] if older else None
        if gd:
            add("- 广东官方每升价：92# **%.2f 元/升**，0# **%.2f 元/升**" % (gd["gas_liter"], gd["diesel_liter"]))
            pgd = (official_prices or {}).get(prev_key) if prev_key else None
            if pgd:
                add("- 广东官方本次调幅：92# **%+.2f 元/升**，0# **%+.2f 元/升**（本脚本折算 %+.3f / %+.3f）" % (
                    gd["gas_liter"] - pgd["gas_liter"], gd["diesel_liter"] - pgd["diesel_liter"],
                    liter_estimate(last["gas"], "gas"), liter_estimate(last["diesel"], "diesel")))
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
        add("- 预估每升：92# 汽油 **%+.2f ~ %+.2f**，95# 汽油 **%+.2f ~ %+.2f**，0# 柴油 **%+.2f ~ %+.2f 元/升**" % (
            liter_estimate(lo, "92"), liter_estimate(hi, "92"),
            liter_estimate(lo, "95"), liter_estimate(hi, "95"),
            liter_estimate(lo, "0"), liter_estimate(hi, "0")))
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
    add("| 公告日 | 方向 | 汽油 | 柴油 | 每升（92# / 95# / 0#） |")
    add("| --- | --- | --- | --- | --- |")
    for n in notices:
        direction = {"up": "上调", "down": "下调", "control": "调控上调", "hold": "不调"}[n["kind"]]
        if n["kind"] == "hold":
            real = realized.get(n["date"])
            est = "%+.2f / - / %+.2f" % (real["gas"], real["diesel"]) if real else "-"
        else:
            est = "%.3f / %.3f / %.3f" % (
                liter_estimate(n["gas"], "92"), liter_estimate(n["gas"], "95"),
                liter_estimate(n["diesel"], "0"))
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
    add("每升折算 = 公告吨幅 × 品质比率 ÷ 升/吨。公告给的是 89# 汽油 / 0# 柴油标准品吨幅，"
        "92#/95# 按品质比率 1.0602 / 1.1200 放大后除以各自升吨比 1322.9 / 1290.4，"
        "柴油比率为 1、1166.7 升/吨。系数由广东省发改委公告附表反推（2026 年 20 期），"
        "对照官方每升价残差均值 ±0.00 元/升、最大 0.01 元/升。实际零售价以加油站为准。")
    return "\n".join(lines)


def report_to_html(md: str) -> str:
    """把 render() 产出的 Markdown 报告转成最小 HTML。

    只覆盖 render() 实际用到的语法：一级/二级标题、引用块、无序列表、表格、加粗、
    分隔线、普通段落。不做通用 Markdown 解析——报告是我们自己拼的，语法集合封闭，
    这里按行处理足够，也避免引入第三方依赖。
    """
    def inline(text: str) -> str:
        """转义 HTML 特殊字符并还原加粗，防止正文里的 < > 破坏结构。"""
        return re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", html_lib.escape(text, quote=False))

    lines = md.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        stripped = lines[i].strip()
        if not stripped:
            i += 1
            continue
        if stripped.startswith("## "):
            out.append("<h2>%s</h2>" % inline(stripped[3:]))
        elif stripped.startswith("# "):
            out.append("<h1>%s</h1>" % inline(stripped[2:]))
        elif stripped.startswith(">"):
            out.append("<blockquote>%s</blockquote>" % inline(stripped[1:].strip()))
        elif stripped.startswith("|"):
            # 连续的 | 行构成一张表：首行表头，次行是 | --- | 分隔行，其余为数据行
            block: list[str] = []
            while i < len(lines) and lines[i].strip().startswith("|"):
                block.append(lines[i].strip())
                i += 1
            rows = [[c.strip() for c in r.strip("|").split("|")] for r in block]
            if len(rows) >= 2 and all(set(c) <= set("-: ") for c in rows[1]):
                head, body = rows[0], rows[2:]
            else:
                head, body = [], rows
            parts = ["<table border='1' cellspacing='0' cellpadding='4'>"]
            if head:
                parts.append("<tr>%s</tr>" % "".join("<th>%s</th>" % inline(c) for c in head))
            for r in body:
                parts.append("<tr>%s</tr>" % "".join("<td>%s</td>" % inline(c) for c in r))
            parts.append("</table>")
            out.append("".join(parts))
            continue
        elif stripped.startswith("- "):
            items: list[str] = []
            while i < len(lines) and lines[i].strip().startswith("- "):
                items.append("<li>%s</li>" % inline(lines[i].strip()[2:]))
                i += 1
            out.append("<ul>%s</ul>" % "".join(items))
            continue
        elif stripped == "---":
            out.append("<hr/>")
        else:
            out.append("<p>%s</p>" % inline(stripped))
        i += 1
    return "\n".join(out)


def notify(title: str, content: str) -> str:
    """推送通知。

    渠道不写死在脚本里：优先走青龙内置 QLAPI.systemNotify，用面板「通知设置」已配好的渠道，
    以后换渠道只改面板即可；QLAPI 不可用时（面板外运行）退回 QINGLONG_WEBHOOK。
    返回一句结果说明，写进日志便于排错。

    默认不传 notificationInfo，渠道与模板全部按面板「通知设置」走：传了 notificationInfo
    会整体替换面板配置，缺 token 等字段时青龙直接返回 code=400，反而发不出去。
    只有显式设置了 NOTIFY_TYPE 时才先试一次带 notificationInfo 的调用，失败再回落面板配置。
    两级都失败时把两段错误一起返回，便于区分是渠道名写错还是面板没配。
    """
    # 正文格式：青龙的 pushplus 渠道实际按 html 模板投递（面板里的 pushplusTemplate 不生效），
    # Markdown 会被当纯文本显示成一堆 # 和 |，所以默认先转成 HTML 再发
    if NOTIFY_TEMPLATE == "html":
        content = report_to_html(content)
    # 官方文档未给 Python 的导入方式，这里两种都试：规范入口是 ql 模块，
    # 旧版执行器则把 QLAPI 直接注入为内置对象（普通解释器里不存在，按名字安全查找）
    qlapi = None
    try:
        from ql import QLAPI as qlapi  # type: ignore[no-redef]
    except Exception:
        qlapi = globals().get("QLAPI") or getattr(builtins, "QLAPI", None)

    if qlapi is not None:
        attempts: list[tuple[str, dict]] = []
        if NOTIFY_TYPE:
            # 声明渠道与 Markdown 模板；其余字段留空，沿用面板里该渠道已存的配置
            attempts.append((
                "指定渠道 %s + template=markdown" % NOTIFY_TYPE,
                {"title": title, "content": content,
                 "notificationInfo": {"type": NOTIFY_TYPE, "template": "markdown"}},
            ))
        attempts.append(("面板默认通知配置", {"title": title, "content": content}))

        errors: list[str] = []
        for desc, params in attempts:
            try:
                resp = qlapi.systemNotify(params)
            except Exception as exc:
                errors.append("%s → 调用异常 %r" % (desc, exc))
                continue
            if isinstance(resp, dict) and resp.get("code") == 200:
                return "已通过青龙通知推送（%s）" % desc
            errors.append("%s → %s" % (desc, resp))
        return "青龙通知推送失败（已试 %d 种方式）：%s" % (len(attempts), "；".join(errors))

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


def main(argv: list[str] | None = None) -> int:
    """生成并打印报告；argv 为 None 时解析命令行参数，便于被青龙入口脚本以固定参数调用。"""
    parser = argparse.ArgumentParser(description="每日国内油价调价速报")
    parser.add_argument("--days", type=int, default=7, help="日历预览天数（默认 7）")
    parser.add_argument("--year-from", type=int, default=None, help="历史起始年份（默认当年）")
    parser.add_argument("--province", choices=["shaanxi", "guangdong"], default=None,
                        help="额外抓取指定省级发改委的官方每升口径，可选 shaanxi（每升调幅）或 guangdong（每升价）")
    parser.add_argument("--no-crude", action="store_true", help="不抓取外盘原油行情（同时跳过下一轮预估）")
    parser.add_argument("--notify", action="store_true",
                        help="输出后推送通知（青龙 QLAPI，或环境变量 QINGLONG_WEBHOOK）")
    parser.add_argument("--json", action="store_true", help="同时输出 JSON 以便二次处理")
    args = parser.parse_args(argv)

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
    # 广东官方每升价：只在显式指定 --province guangdong 时抓取，与陕西口径互斥；
    # 抓取失败不影响主流程（render 里按空值自动省略该行）
    official_prices = collect_guangdong_prices(notices) if args.province == "guangdong" else {}
    crude = [] if args.no_crude else fetch_crude()
    # 预估依赖外盘日线，--no-crude 视作「只看官方数据」，一并跳过
    forecast = None if args.no_crude else forecast_next(notices, cycle, today)

    report = render(notices, cycle, realized, crude, today, args.days, forecast, official_prices)
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
    # 由青龙面板执行器运行时，ql 模块（或内置 QLAPI）存在，说明这次是要推送报告的任务，
    # 自动补上 --notify，省得在面板命令里再写一遍；命令行手动运行时保持原样，只打印不打扰。
    _args = sys.argv[1:]
    if "--notify" not in _args and (getattr(builtins, "QLAPI", None) is not None
                                    or "ql" in sys.modules):
        _args.append("--notify")
    sys.exit(main(_args))
