#!/usr/bin/env python3
"""
株式会社Winforce 経営ダッシュボード更新スクリプト
毎朝7時にGitHub Actionsから自動実行される

データ取得:
  - 売上データ  → Googleスプレッドシート（CSV）
  - 経営分析    → Claude API（Chatworkログ + 財務データを解析）
  - Chatwork    → 2つのAPIアカウントから各事業のグループチャットを取得
"""

import os
import io
import csv
import json
import re
import base64
import secrets as pysecrets
import requests
from datetime import datetime, timezone, timedelta
import anthropic
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

# ============================================================
# 設定
# ============================================================
CW_TOKEN_1     = os.environ['CHATWORK_API_TOKEN_1']
CW_TOKEN_2     = os.environ['CHATWORK_API_TOKEN_2']
CLAUDE_API_KEY = os.environ['CLAUDE_API_KEY']
SHEET_ID       = os.environ['SHEET_ID']
DASHBOARD_PW   = os.environ['DASHBOARD_PASSWORD']
ORG_SHEET_ID   = os.environ.get('ORG_SHEET_ID', '')  # 組織図スプレッドシート（任意）

# くまお(5501140) = YutoKato(10153653)：同一人物（代表取締役）のアカウントID統合
MERGE_ACCOUNTS = {5501140: 10153653}

JST = timezone(timedelta(hours=9))
CW_BASE = 'https://api.chatwork.com/v2'

# ============================================================
# ★★★ Chatworkルーム設定（ルームIDを設定してください）★★★
# ルームIDの確認方法:
#   Chatworkを開いてグループチャットを選択し、
#   URLの #!rid の後の数字がルームIDです
#   例: https://www.chatwork.com/#!rid123456789 → ルームID: 123456789
# ============================================================
CHATWORK_ROOMS = [
    # ---- メディア運用事業 ----
    {'token': 'TOKEN_1', 'room_id': '339645149', 'biz_id': 'media',     'name': '【宇崎さん】プロジェクト進行チャット'},
    {'token': 'TOKEN_1', 'room_id': '421984121', 'biz_id': 'media',     'name': '【WF内部】工場改善サービス様_制作対応チャット'},
    {'token': 'TOKEN_1', 'room_id': '422224663', 'biz_id': 'media',     'name': '【工場改善サービス様】全体連絡チャット'},
    {'token': 'TOKEN_2', 'room_id': '421509838', 'biz_id': 'media',     'name': '【WF】SKリンク様_採用支援プロジェクト'},
    {'token': 'TOKEN_1', 'room_id': '423811546', 'biz_id': 'media',     'name': '【WF】PSF法律事務所様_制作対応チャット'},
    # ---- 物流事業 ----
    {'token': 'TOKEN_2', 'room_id': '422457076', 'biz_id': 'logistics', 'name': '【ステップワン様】対応チャット'},
    {'token': 'TOKEN_2', 'room_id': '425486645', 'biz_id': 'logistics', 'name': '【WF】アートセッティングデリバリー様_対応チャット'},
    {'token': 'TOKEN_2', 'room_id': '414959930', 'biz_id': 'logistics', 'name': '【WF】松永種苗様_対応チャット'},
    {'token': 'TOKEN_2', 'room_id': '425014098', 'biz_id': 'logistics', 'name': '【WF】日工様_業務報告チャット'},
    {'token': 'TOKEN_2', 'room_id': '417850620', 'biz_id': 'logistics', 'name': '【WF】ユニマット様_対応チャット'},
    {'token': 'TOKEN_2', 'room_id': '425911292', 'biz_id': 'logistics', 'name': '【WF】シフト調整チャット'},
    {'token': 'TOKEN_2', 'room_id': '414960040', 'biz_id': 'logistics', 'name': '【WF】Amazon対応チャット'},
    # ---- 経営企画事業：Chatwork監視なし ----
    # ---- オンライン秘書事業 ----
    {'token': 'TOKEN_1', 'room_id': '420733406', 'biz_id': 'secretary',   'name': '【WF】オンライン秘書事業_構築チャット'},
    # ---- 運営（全社横断）----
    {'token': 'TOKEN_1', 'room_id': '336833853', 'biz_id': 'management',  'name': '飯田ここさんとのDM'},
    {'token': 'TOKEN_1', 'room_id': '416199200', 'biz_id': 'management',  'name': '【WF】岡本さんとやり取りするチャット'},
    # ---- 物流事業（追加）----
    {'token': 'TOKEN_2', 'room_id': '425724017', 'biz_id': 'logistics',   'name': '【WF】希望休通知チャット'},
    {'token': 'TOKEN_2', 'room_id': '412105315', 'biz_id': 'logistics',   'name': '【WF】車両管理'},
]

# 事業マスター
BUSINESSES = [
    {'id': 'media',     'name': 'メディア運用事業', 'color': '#3b82f6'},
    {'id': 'planning',  'name': '経営企画事業',     'color': '#8b5cf6'},
    {'id': 'logistics', 'name': '物流事業',         'color': '#f97316'},
    {'id': 'secretary', 'name': 'オンライン秘書事業','color': '#10b981'},
]

# ============================================================
# ① Googleスプレッドシートから財務データ取得
# ============================================================
def fetch_org_chart() -> list:
    """組織図スプレッドシートを取得してパース"""
    if not ORG_SHEET_ID:
        return []
    url = f'https://docs.google.com/spreadsheets/d/{ORG_SHEET_ID}/export?format=csv'
    try:
        resp = requests.get(url, allow_redirects=True, timeout=30)
        if resp.status_code != 200:
            print(f'[WARN] 組織図スプレッドシート HTTP {resp.status_code}')
            return []
        resp.encoding = 'utf-8'
        reader = csv.reader(io.StringIO(resp.text))
        return list(reader)
    except Exception as e:
        print(f'[ERROR] fetch_org_chart: {e}')
        return []


def build_account_map(org_rows: list) -> dict:
    """組織図からaccount_id → スタッフ情報マッピングを構築
    列: A=氏名, B=事業部, C=役職, D=雇用形態, E=CW_account_id, F=CW表示名, G=備考
    """
    account_map = {}  # account_id(int) -> {'name':str, 'dept':str, 'role':str, 'employment':str}
    if not org_rows:
        return account_map

    for row in org_rows[1:]:  # ヘッダー行スキップ
        if len(row) < 5:
            continue
        name       = row[0].strip()
        dept       = row[1].strip()
        role       = row[2].strip()
        employment = row[3].strip() if len(row) > 3 else ''
        cw_id_str  = row[4].strip()

        if not cw_id_str or not name:
            continue
        try:
            cw_id = int(cw_id_str)
        except ValueError:
            continue

        info = {'name': name, 'dept': dept, 'role': role, 'employment': employment}
        # マージ処理：くまお → YutoKato
        primary_id = MERGE_ACCOUNTS.get(cw_id, cw_id)
        account_map[cw_id] = info
        if primary_id != cw_id:
            account_map[primary_id] = info  # 両方のIDで参照可能に

    # くまお/YutoKato などのマージアカウントで名前を統一
    # secondary(key) の名前を canonical として primary(value) に適用
    for secondary_id, primary_id in MERGE_ACCOUNTS.items():
        if secondary_id in account_map and primary_id in account_map:
            canonical_name = account_map[secondary_id]['name']
            account_map[primary_id]['name'] = canonical_name

    return account_map


def _build_all_staff_roster(account_map: dict) -> list:
    """全スタッフ一覧リストを構築（重複除去・名前順）"""
    seen, roster = set(), []
    for info in account_map.values():
        name = info['name']
        if name in seen:
            continue
        seen.add(name)
        roster.append({'name': name, 'dept': info['dept'], 'role': info['role']})
    roster.sort(key=lambda x: (x['dept'], x['name']))
    return roster


def build_staff_by_dept(account_map: dict) -> dict:
    """事業部ごとのスタッフ一覧を構築（Claudeプロンプト用）"""
    dept_map = {}
    seen = set()
    for acc_id, info in account_map.items():
        key = (info['name'], info['dept'])
        if key in seen:
            continue
        seen.add(key)
        dept = info['dept']
        if dept not in dept_map:
            dept_map[dept] = []
        dept_map[dept].append(f"{info['name']}（{info['role']}・{info['employment']}）")
    return dept_map


def fetch_spreadsheet():
    """スプレッドシートのCSVを取得してパース"""
    url = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv'
    try:
        resp = requests.get(url, allow_redirects=True, timeout=30)
        if resp.status_code != 200:
            print(f'[WARN] Spreadsheet HTTP {resp.status_code}')
            return []
        resp.encoding = 'utf-8'
        reader = csv.reader(io.StringIO(resp.text))
        return list(reader)
    except Exception as e:
        print(f'[ERROR] fetch_spreadsheet: {e}')
        return []


def get_month_col(month: int) -> int:
    """月（1-12）をスプレッドシートの列インデックス（0始まり）に変換
    列D（インデックス3）= 1月, E（4）= 2月, ..., O（14）= 12月
    """
    return month + 2  # 1月=3, 2月=4, ..., 12月=14


def parse_num(val: str) -> float:
    """数値文字列をfloatに変換（カンマ・¥・%・#DIV/0! 対応）"""
    if not val:
        return 0.0
    v = val.replace(',', '').replace('¥', '').strip()
    if '#DIV/0!' in v or '#' in v:
        return 0.0
    # パーセント（例: 100.00%）
    if v.endswith('%'):
        try:
            return float(v[:-1])
        except ValueError:
            return 0.0
    try:
        return float(v)
    except ValueError:
        return 0.0


def get_col(row: list, idx: int) -> float:
    """行から安全に数値取得"""
    if idx >= len(row):
        return 0.0
    return parse_num(row[idx])


ANNUAL_COL = 15  # 合計列（列P、0インデックス）


def parse_financials(rows: list, month: int) -> dict:
    """スプレッドシートを解析して各事業の財務データを抽出"""
    mc = get_month_col(month)  # 当月の列インデックス

    # スプレッドシート構造:
    #   列A=事業名, 列B=大項目, 列C=小項目, 列D〜O=1月〜12月の数値
    #
    # セクション構造（各事業10〜12行）:
    #   1. （空）| 売上高 | （空） | 数値...
    #   2. （空）| 収入(売掛金回収) | ...
    #   3. [事業名] | （空） | 労務費(外注) | 数値...
    #   4. 事業部 | （空） | 固定費 | 数値...
    #   5. （空）| （空） | 変動費 | 数値...
    #   6. （空）| 経費合計 | （空） | 数値...
    #   7. （空）| 粗利 | （空） | 数値...
    #   8. （空）| 粗利率 | （空） | %値...
    #   9. （空）| 受注件数 | ...
    #  10. （空）| 営業利益 | （空） | 数値...
    #  11. （空）| 営業利益率 | （空） | %値...
    #
    # 各事業のセクション開始はrow[0]が事業名と一致する行の直前にある売上高行

    SECTION_STARTERS = {
        'メディア':   'media',
        '経営企画':   'planning',
        '物流':       'logistics',
        'オン秘書':   'secretary',
    }

    result = {}
    current_biz_id = None
    current_revenue = None
    current_revenue_annual = None
    current_revenue_monthly = None

    for i, row in enumerate(rows):
        col_a = row[0].strip() if len(row) > 0 else ''
        col_b = row[1].strip() if len(row) > 1 else ''
        col_c = row[2].strip() if len(row) > 2 else ''

        # 全社合計セクション（利益計算）
        if '売上高(全事業合計)' in col_b:
            result['_overall_revenue'] = get_col(row, mc)
            result['_overall_revenue_annual'] = get_col(row, ANNUAL_COL)
            result['_overall_revenue_monthly'] = [get_col(row, get_month_col(m)) for m in range(1, 13)]
        if '純利益' in col_b:
            result['_overall_op_profit'] = get_col(row, mc)
            result['_overall_op_profit_annual'] = get_col(row, ANNUAL_COL)
            result['_overall_op_profit_monthly'] = [get_col(row, get_month_col(m)) for m in range(1, 13)]
        if '売上総利益' in col_b:
            result['_overall_gross_profit'] = get_col(row, mc)
            result['_overall_gross_profit_annual'] = get_col(row, ANNUAL_COL)
        if col_c == '営業利益率' and col_b == '':
            result['_overall_op_profit_rate_monthly'] = [round(get_col(row, get_month_col(m)), 2) for m in range(1, 13)]

        # 「売上高」行 → 次に来る事業名のための仮保存
        if col_b == '売上高' and col_a == '':
            current_revenue = get_col(row, mc)
            current_revenue_annual = get_col(row, ANNUAL_COL)
            current_revenue_monthly = [get_col(row, get_month_col(m)) for m in range(1, 13)]
            continue

        # 運営経費セクション検出 → 事業別処理を終了
        if col_a == '運営経費' or col_b in ['メディア事業部', '経営企画事業部', 'オン秘書事業部', '軽貨物事業部']:
            current_biz_id = None
            continue

        # 事業名検出
        if col_a in SECTION_STARTERS:
            current_biz_id = SECTION_STARTERS[col_a]
            if current_biz_id not in result:
                result[current_biz_id] = {
                    'revenue': current_revenue or 0.0,
                    'revenueAnnual': current_revenue_annual or 0.0,
                    'revenueMonthly': current_revenue_monthly or [0.0]*12,
                    'laborCost': 0.0,
                    'fixedCost': 0.0,
                    'variableCost': 0.0,
                    'expenses': 0.0,
                    'grossProfit': 0.0,
                    'grossProfitRate': 0.0,
                    'opProfit': 0.0,
                    'opProfitRate': 0.0,
                    'opProfitAnnual': 0.0,
                    'opProfitMonthly': [0.0]*12,
                    'opProfitRateMonthly': [0.0]*12,
                }
            # この行は労務費行
            if '労務費' in col_c:
                result[current_biz_id]['laborCost'] = get_col(row, mc)
            current_revenue = None
            continue

        # 全社運営経費の内訳（事業セクション外でも抽出）
        if col_b == '運営固定経費':
            result['_ops_fixed'] = get_col(row, mc)
            result['_ops_fixed_monthly'] = [get_col(row, get_month_col(m)) for m in range(1, 13)]
        if col_b == '運営変動経費':
            result['_ops_variable'] = get_col(row, mc)
            result['_ops_variable_monthly'] = [get_col(row, get_month_col(m)) for m in range(1, 13)]
        if '運営労務費' in col_b:
            result['_ops_labor'] = get_col(row, mc)
            result['_ops_labor_monthly'] = [get_col(row, get_month_col(m)) for m in range(1, 13)]

        if current_biz_id is None:
            continue

        biz = result[current_biz_id]

        if col_a == '事業部' and '固定費' in col_c:
            biz['fixedCost'] = get_col(row, mc)
        elif col_a == '' and '変動費' in col_c:
            biz['variableCost'] = get_col(row, mc)
        elif col_b == '経費合計':
            biz['expenses'] = get_col(row, mc)
        elif col_b == '粗利' and col_c == '':
            biz['grossProfit'] = get_col(row, mc)
        elif col_b == '粗利率':
            biz['grossProfitRate'] = round(get_col(row, mc), 2)
        elif col_b == '営業利益' and col_c == '':
            biz['opProfit'] = get_col(row, mc)
            biz['opProfitAnnual'] = get_col(row, ANNUAL_COL)
            biz['opProfitMonthly'] = [get_col(row, get_month_col(m)) for m in range(1, 13)]
        elif col_b == '営業利益率':
            biz['opProfitRate'] = round(get_col(row, mc), 2)
            biz['opProfitRateMonthly'] = [round(get_col(row, get_month_col(m)), 2) for m in range(1, 13)]
        elif '労務費合計' in col_c:
            result['_overall_labor'] = get_col(row, mc)
        elif col_b == '売上高' and col_a == '':
            # 次の事業の売上高
            current_revenue = get_col(row, mc)
            current_biz_id = None  # リセット（次の事業名待ち）

    return result


# ============================================================
# ② Chatwork APIからメッセージ取得
# ============================================================
def get_chatwork_messages(token: str, room_id: str) -> list:
    """指定ルームのメッセージを取得"""
    if room_id.startswith('ROOM_ID_'):
        print(f'  [SKIP] ルームID未設定: {room_id}')
        return []
    try:
        resp = requests.get(
            f'{CW_BASE}/rooms/{room_id}/messages',
            headers={'X-ChatWorkToken': token},
            params={'force': 1},
            timeout=30
        )
        if resp.status_code == 200:
            return resp.json()
        print(f'[WARN] Chatwork {resp.status_code} room={room_id}')
        return []
    except Exception as e:
        print(f'[ERROR] get_chatwork_messages({room_id}): {e}')
        return []


def sanitize(text: str) -> str:
    """JSON埋め込み時に問題となる文字を除去"""
    return text.replace('\\', '').replace('"', '').replace('\r', '').replace('\x00', '')


def format_messages(msgs: list, room_name: str, account_map: dict = None) -> str:
    """メッセージリストを分析用テキストに整形（直近50件）
    account_mapがある場合は発言者名を付与する
    """
    lines = [f'\n=== {room_name} ===']
    recent = msgs[-50:] if len(msgs) > 50 else msgs
    for msg in recent:
        dt = datetime.fromtimestamp(msg.get('send_time', 0), tz=JST)
        body = sanitize(msg.get('body', '').strip())
        if not body:
            continue
        acc_id = msg.get('account', {}).get('account_id', 0)
        # account_mapがある場合はスタッフ名を付与
        if account_map and acc_id in account_map:
            sender = account_map[acc_id]['name']
        else:
            sender = msg.get('account', {}).get('name', '')
        lines.append(f'[{dt.strftime("%m/%d %H:%M")}][{sender}] {body}')
    return '\n'.join(lines)


# ============================================================
# ③ Claude APIで経営分析
# ============================================================
def analyze_with_claude(financials: dict, chatwork_logs: dict, month_str: str,
                        staff_by_dept: dict = None) -> dict:
    """Claude APIで全事業の経営分析を実行"""
    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)

    # コンテキスト構築
    ctx_lines = [f'【対象月】{month_str}', '', '【財務データ】']
    for biz in BUSINESSES:
        bid = biz['id']
        f = financials.get(bid, {})
        ctx_lines.append(f"\n■ {biz['name']}")
        rev = f.get('revenue', 0)
        var = f.get('variableCost', 0)
        var_rate = round(var / rev * 100, 1) if rev > 0 else 0
        ctx_lines.append(f"  売上: {rev:,.0f}円 / 経費合計: {f.get('expenses',0):,.0f}円")
        ctx_lines.append(f"  粗利: {f.get('grossProfit',0):,.0f}円 ({f.get('grossProfitRate',0):.1f}%)")
        ctx_lines.append(f"  営業利益: {f.get('opProfit',0):,.0f}円 ({f.get('opProfitRate',0):.1f}%)")
        ctx_lines.append(f"  労務費: {f.get('laborCost',0):,.0f}円 / 固定費: {f.get('fixedCost',0):,.0f}円")
        ctx_lines.append(f"  変動費（交通費・接待交際費等）: {var:,.0f}円（売上比 {var_rate}%）")

    # スタッフ一覧（組織図が設定されている場合）
    if staff_by_dept:
        ctx_lines.append('\n【スタッフ一覧（組織図）】')
        for dept, members in staff_by_dept.items():
            ctx_lines.append(f'  {dept}: {", ".join(members)}')
        ctx_lines.append('  ※Chatworkログの[名前]タグで発言者を特定できます')

    ctx_lines.append('\n【Chatworkログ（[発言者名]付き）】')
    if chatwork_logs:
        for biz in BUSINESSES:
            bid = biz['id']
            if bid in chatwork_logs and chatwork_logs[bid]:
                for room_text in chatwork_logs[bid]:
                    ctx_lines.append(room_text)
        # 運営チャット（全社横断）
        if 'management' in chatwork_logs and chatwork_logs['management']:
            ctx_lines.append('\n--- 運営チャット（全社横断）---')
            for room_text in chatwork_logs['management']:
                ctx_lines.append(room_text)
    else:
        ctx_lines.append('（Chatworkルームが未設定のためログなし）')

    context = '\n'.join(ctx_lines)

    prompt = f"""あなたは株式会社Winforceの経営コンサルタントです。
{month_str}時点のWinforce各事業の財務データとChatworkログを分析し、以下のJSON形式で返してください。

{context}

---
【出力ルール】
- 全テキスト項目は60字以内で簡潔に記述
- topRisks・goodPoints・improvements・risksは最大3件まで
- staffStatusは組織図記載のスタッフ全員分を必ず記入（件数制限なし）
- JSONのみ返す（コードブロック不要）
- overallSummaryのみ150字以内

{{
  "overallSummary": "全社経営状況の総括（150字以内）",
  "topRisks": ["全社リスク1（60字以内）", "全社リスク2"],
  "actionPlans": {{
    "month1": ["直近1ヶ月のアクション1（誰が・何を）", "アクション2", "アクション3"],
    "month3": ["3ヶ月以内のアクション1", "アクション2", "アクション3"],
    "month6": ["6ヶ月以内のアクション1", "アクション2", "アクション3"]
  }},
  "overallStaffStatus": [
    {{"name": "くまお", "status": "good", "note": "運営スタッフの状況"}},
    {{"name": "飯田ここ", "status": "good", "note": ""}},
    {{"name": "岡本あゆみ", "status": "unknown", "note": ""}},
    {{"name": "中西稜", "status": "unknown", "note": ""}}
  ],
  "businesses": {{
    "media": {{
      "financialAnalysis": "メディア運用事業の財務分析（60字以内）",
      "goodPoints": ["良い点1", "良い点2"],
      "improvements": ["改善点1", "改善点2"],
      "risks": ["リスク1", "リスク2"],
      "staffStatus": [
        {{"name": "宇崎こうた", "status": "good", "note": ""}},
        {{"name": "吉永鉄", "status": "good", "note": ""}},
        {{"name": "パパすけ", "status": "good", "note": ""}},
        {{"name": "宇井警太", "status": "good", "note": ""}}
      ]
    }},
    "planning": {{
      "financialAnalysis": "経営企画事業の財務分析",
      "goodPoints": [], "improvements": [], "risks": [], "staffStatus": []
    }},
    "logistics": {{
      "financialAnalysis": "物流事業の財務分析",
      "goodPoints": [], "improvements": [], "risks": [],
      "staffStatus": [
        {{"name": "すぎしょう", "status": "good", "note": ""}},
        {{"name": "まるお", "status": "good", "note": ""}},
        {{"name": "木村一樹", "status": "good", "note": ""}},
        {{"name": "加藤尚斗", "status": "good", "note": ""}},
        {{"name": "小川勇司", "status": "good", "note": ""}},
        {{"name": "勝田洸誠", "status": "good", "note": ""}},
        {{"name": "小村星大", "status": "good", "note": ""}}
      ]
    }},
    "secretary": {{
      "financialAnalysis": "オンライン秘書事業の財務分析",
      "goodPoints": [], "improvements": [], "risks": [],
      "staffStatus": [{{"name": "小原千怜", "status": "good", "note": ""}}]
    }}
  }}
}}

【スタッフ分析ルール】
- 上記staffStatusは「雛形」です。Chatworkログ（[氏名]タグで発言者を特定）を必ず参照して各自の状況を上書きしてください
- Chatworkログに発言が見つかったスタッフ: 発言内容・トーン・業務状況からstatus（good/warning/concern）とnoteを記入
- Chatworkログに発言がないスタッフ: status "unknown", note "Chatworkログに発言なし" のままにしてください
- overallStaffStatusは運営スタッフ（くまお・飯田ここ・岡本あゆみ・中西稜）を対象に、運営チャットログから分析してください
【変動費アラート】変動費（交通費・接待交際費等）の売上比率が3%超の場合はrisksに含めてください（0円は不要）。"""

    try:
        message = client.messages.create(
            model='claude-sonnet-4-6',
            max_tokens=8192,
            messages=[{'role': 'user', 'content': prompt}]
        )
        text = message.content[0].text.strip()
        # コードブロック除去
        text = re.sub(r'^```(?:json)?\s*', '', text)
        text = re.sub(r'\s*```$', '', text)
        # JSONブロック抽出（最初の { から最後の } まで）
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1:
            json_str = text[start:end+1]
            return json.loads(json_str)
    except json.JSONDecodeError as e:
        print(f'[ERROR] Claude API JSON parse: {e}')
        print(f'[DEBUG] Response text (first 500 chars): {text[:500]}')
    except Exception as e:
        print(f'[ERROR] Claude API: {e}')

    return {
        'overallSummary': '分析データを取得できませんでした。次回の実行をお待ちください。',
        'topRisks': [],
        'actionPlans': {'month1': [], 'month3': [], 'month6': []},
        'businesses': {
            biz['id']: {
                'financialAnalysis': '', 'goodPoints': [],
                'improvements': [], 'risks': [], 'staffStatus': []
            } for biz in BUSINESSES
        }
    }


# ============================================================
# ④ データ暗号化
# ============================================================
SALT = b'wf_report_2026__'  # 16bytes固定（index.htmlと同じ）

def encrypt_data(data_dict: dict, password: str) -> str:
    """データをAES-GCMで暗号化してbase64エンコード"""
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=SALT,
        iterations=100000,
    )
    key = kdf.derive(password.encode('utf-8'))
    nonce = pysecrets.token_bytes(12)
    aesgcm = AESGCM(key)
    plaintext = json.dumps(data_dict, ensure_ascii=False).encode('utf-8')
    ciphertext = aesgcm.encrypt(nonce, plaintext, None)
    return base64.b64encode(nonce + ciphertext).decode('ascii')


# ============================================================
# メイン
# ============================================================
def main():
    now = datetime.now(JST)
    print(f'=== 更新開始: {now.strftime("%Y-%m-%d %H:%M JST")} ===')

    month = now.month
    month_str = now.strftime('%Y年%m月')

    # 0. 組織図取得
    account_map = {}
    staff_by_dept = {}
    if ORG_SHEET_ID:
        print('組織図スプレッドシートを取得中...')
        org_rows = fetch_org_chart()
        if org_rows:
            account_map = build_account_map(org_rows)
            staff_by_dept = build_staff_by_dept(account_map)
            print(f'  スタッフ {len(set(v["name"] for v in account_map.values()))}名 読み込み完了')
        else:
            print('[WARN] 組織図の取得に失敗しました')

    # 1. スプレッドシート取得
    print('スプレッドシートから財務データを取得中...')
    rows = fetch_spreadsheet()
    if rows:
        financials = parse_financials(rows, month)
        print(f'  取得完了（{len(rows)}行）')
    else:
        financials = {}
        print('[WARN] スプレッドシートの取得に失敗しました')

    # 2. Chatworkメッセージ取得
    print('Chatworkメッセージ取得中...')
    chatwork_logs = {}
    token_map = {'TOKEN_1': CW_TOKEN_1, 'TOKEN_2': CW_TOKEN_2}

    all_accounts = {}  # account_id -> {'name': ..., 'rooms': set()}

    for room_cfg in CHATWORK_ROOMS:
        token = token_map[room_cfg['token']]
        room_id = room_cfg['room_id']
        biz_id = room_cfg['biz_id']

        msgs = get_chatwork_messages(token, room_id)
        print(f"  {room_cfg['name']}: {len(msgs)}件")

        # account_id収集（組織図スプレッドシート作成用）
        for msg in msgs:
            acc = msg.get('account', {})
            acc_id = acc.get('account_id')
            if acc_id:
                if acc_id not in all_accounts:
                    all_accounts[acc_id] = {'name': acc.get('name', ''), 'rooms': set()}
                all_accounts[acc_id]['rooms'].add(room_cfg['name'])

        if msgs:
            text = format_messages(msgs, room_cfg['name'], account_map)
            if biz_id not in chatwork_logs:
                chatwork_logs[biz_id] = []
            chatwork_logs[biz_id].append(text)

    # Chatwork発言者一覧を表示（組織図スプレッドシート作成時に参照）
    print('\n=== Chatwork発言者一覧（組織図スプレッドシート作成用）===')
    for acc_id, info in sorted(all_accounts.items()):
        rooms = ' / '.join(sorted(info['rooms']))
        print(f'  account_id: {acc_id}  名前: {info["name"]}  ルーム: {rooms}')
    print('=== 一覧終わり ===\n')

    # 3. Claude分析
    print('Claude APIで経営分析中...')
    analysis = analyze_with_claude(financials, chatwork_logs, month_str, staff_by_dept)

    # 4. データ構築
    # 全社合計
    overall_revenue     = financials.get('_overall_revenue', 0)
    overall_gross       = financials.get('_overall_gross_profit', 0)
    overall_op          = financials.get('_overall_op_profit', 0)
    overall_labor       = financials.get('_overall_labor', 0)
    overall_expenses    = sum(financials.get(b['id'], {}).get('expenses', 0) for b in BUSINESSES)
    gross_profit_rate   = round(overall_gross / overall_revenue * 100, 2) if overall_revenue > 0 else 0
    op_profit_rate      = round(overall_op / overall_revenue * 100, 2) if overall_revenue > 0 else 0

    biz_analysis = analysis.get('businesses', {})

    data = {
        'updatedAt':       now.isoformat(),
        'updatedAtLabel':  now.strftime('%Y年%m月%d日 %H:%M'),
        'targetMonth':     month_str,
        'overallSummary':      analysis.get('overallSummary', ''),
        'topRisks':            analysis.get('topRisks', []),
        'actionPlans':         analysis.get('actionPlans', {'month1': [], 'month3': [], 'month6': []}),
        'overallStaffStatus':  analysis.get('overallStaffStatus', []),
        'overall': {
            'totalRevenue':        overall_revenue,
            'totalRevenueAnnual':  financials.get('_overall_revenue_annual', 0),
            'totalLaborCost':      overall_labor,
            'totalExpenses':       overall_expenses,
            'grossProfit':         overall_gross,
            'grossProfitAnnual':   financials.get('_overall_gross_profit_annual', 0),
            'grossProfitRate':     gross_profit_rate,
            'opProfit':            overall_op,
            'opProfitAnnual':      financials.get('_overall_op_profit_annual', 0),
            'opProfitRate':        op_profit_rate,
            'opsFixedCost':        financials.get('_ops_fixed', 0),
            'opsVariableCost':     financials.get('_ops_variable', 0),
            'opsLaborCost':        financials.get('_ops_labor', 0),
            'monthly': [
                {
                    'month':        m,
                    'label':        f'{m}月',
                    'revenue':      financials.get('_overall_revenue_monthly', [0]*12)[m-1],
                    'opProfit':     financials.get('_overall_op_profit_monthly', [0]*12)[m-1],
                    'opProfitRate': financials.get('_overall_op_profit_rate_monthly', [0]*12)[m-1],
                    'isForecast':   m > month,
                }
                for m in range(1, 13)
            ],
        },
        'businesses': [],
        'allStaffRoster': _build_all_staff_roster(account_map),
    }

    for biz in BUSINESSES:
        bid = biz['id']
        f = financials.get(bid, {})
        a = biz_analysis.get(bid, {})
        data['businesses'].append({
            'id':    bid,
            'name':  biz['name'],
            'color': biz['color'],
            'sales': {
                'revenue':              f.get('revenue', 0),
                'revenueAnnual':        f.get('revenueAnnual', 0),
                'laborCost':            f.get('laborCost', 0),
                'fixedCost':            f.get('fixedCost', 0),
                'variableCost':         f.get('variableCost', 0),
                'expenses':             f.get('expenses', 0),
                'grossProfit':          f.get('grossProfit', 0),
                'grossProfitRate':      f.get('grossProfitRate', 0),
                'opProfit':             f.get('opProfit', 0),
                'opProfitAnnual':       f.get('opProfitAnnual', 0),
                'opProfitRate':         f.get('opProfitRate', 0),
                'monthly': [
                    {
                        'month':        m,
                        'label':        f'{m}月',
                        'revenue':      f.get('revenueMonthly', [0]*12)[m-1],
                        'opProfit':     f.get('opProfitMonthly', [0]*12)[m-1],
                        'opProfitRate': f.get('opProfitRateMonthly', [0]*12)[m-1],
                        'isForecast':   m > month,
                    }
                    for m in range(1, 13)
                ],
            },
            'analysis': {
                'financialAnalysis': a.get('financialAnalysis', ''),
                'goodPoints':        a.get('goodPoints', []),
                'improvements':      a.get('improvements', []),
                'risks':             a.get('risks', []),
                'staffStatus':       a.get('staffStatus', []),
            }
        })

    # 5. 暗号化して保存
    print('データを暗号化して保存中...')
    encrypted = encrypt_data(data, DASHBOARD_PW)
    with open('data.enc', 'w', encoding='ascii') as f:
        f.write(encrypted)

    print('=== 完了 ===')
    print(f'  全社売上: ¥{overall_revenue:,.0f}')
    print(f'  全社粗利: ¥{overall_gross:,.0f} ({gross_profit_rate:.1f}%)')
    print(f'  純利益:   ¥{overall_op:,.0f} ({op_profit_rate:.1f}%)')


if __name__ == '__main__':
    main()
