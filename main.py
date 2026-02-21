import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from tqdm import tqdm
import os
from datetime import datetime, timedelta, timezone
import asyncio
import aiohttp
import nest_asyncio
import re
import random

nest_asyncio.apply()

def set_naver_custom_fields(session, field_ids):
    url = "https://finance.naver.com/sise/field_submit.naver"
    params = [('menu', 'market_sum'), ('returnUrl', 'http://finance.naver.com/sise/sise_market_sum.naver')]
    for fid in field_ids:
        params.append(('fieldIds', fid))
    session.get(url, params=params)

def crawl_market_sum(session, desc_label):
    base_url = "https://finance.naver.com/sise/sise_market_sum.naver?sosok={}&page={}"
    result_df = pd.DataFrame()
    
    for sosok in [0, 1]:
        market_name = 'KOSPI' if sosok == 0 else 'KOSDAQ'
        for page in tqdm(range(1, 45), desc=f"{desc_label} - {market_name}"):
            res = session.get(base_url.format(sosok, page))
            soup = BeautifulSoup(res.text, 'html.parser')
            table = soup.find('table', {'class': 'type_2'})
            
            if not table: continue
            
            try:
                df = pd.read_html(StringIO(str(table)))[0]
            except ValueError:
                break
                
            df = df.dropna(subset=['종목명'])
            links = table.find_all('a', class_='tltle')
            codes = [link['href'].split('code=')[-1] for link in links]
            
            if len(codes) == len(df):
                df['종목코드'] = codes
            else:
                continue
                
            df = df.drop(columns=['N', '토론실'], errors='ignore')
            result_df = pd.concat([result_df, df], ignore_index=True)
            if len(df) < 10: break
            
    return result_df.drop_duplicates(subset=['종목코드']).reset_index(drop=True)

async def fetch_investor(session, code, sem):
    async with sem:
        url = f"https://finance.naver.com/item/frgn.naver?code={code}"
        try:
            await asyncio.sleep(random.uniform(0.1, 0.4))
            
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    text = await response.text()
                    soup = BeautifulSoup(text, 'html.parser')
                    
                    tables = soup.find_all('table', {'class': 'type2'})
                    for table in tables:
                        rows = table.find_all('tr')
                        for row in rows:
                            tds = row.find_all('td')
                            if len(tds) >= 9:
                                date_text = tds[0].text.strip()
                                if len(date_text) == 10 and date_text.count('.') == 2:
                                    inst_net = tds[5].text.strip()
                                    fore_net = tds[6].text.strip()
                                    fore_ratio = tds[8].text.strip()
                                    return code, inst_net, fore_net, fore_ratio
        except Exception:
            pass
    return code, '0', '0', '0.00'

async def get_all_investors(codes):
    print("\n[비동기] 기관/외국인 수급 및 보유율 데이터를 수집합니다... (약 1~2분 소요)")
    results = []
    sem = asyncio.Semaphore(15)
    
    connector = aiohttp.TCPConnector(limit=15)
    async with aiohttp.ClientSession(connector=connector, headers={'User-Agent': 'Mozilla/5.0'}) as session:
        tasks = [fetch_investor(session, code, sem) for code in codes]
        for task in tqdm(asyncio.as_completed(tasks), total=len(codes), desc="수급 수집"):
            res = await task
            results.append(res)
    return results

def get_full_market_data():
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    # 1차 데이터: 보통주배당금(dividend) 포함
    group1 = ['sales', 'operating_profit', 'net_income', 'property_total', 'debt_total', 'dividend']
    set_naver_custom_fields(session, group1)
    df1 = crawl_market_sum(session, "1차 데이터 수집")
    
    # [수정] 2차 데이터: 상장주식수(listed_stock_cnt) 제거
    group2 = ['market_sum', 'per', 'pbr', 'quant']
    set_naver_custom_fields(session, group2)
    df2 = crawl_market_sum(session, "2차 데이터 수집")
    
    common_cols = ['종목코드', '종목명']
    merged_df = pd.merge(df1, df2.drop(columns=['현재가', '전일비', '등락률'], errors='ignore'), on=common_cols, how='left')
    
    loop = asyncio.get_event_loop()
    investor_data = loop.run_until_complete(get_all_investors(merged_df['종목코드'].tolist()))
    inv_df = pd.DataFrame(investor_data, columns=['종목코드', '기관 순매매량', '외국인 순매매량', '외국인 보유율(%)'])
    merged_df = pd.merge(merged_df, inv_df, on='종목코드', how='left')
    
    print("\n수집된 데이터를 바탕으로 재무비율을 계산합니다...")
    
    div_col = next((c for c in merged_df.columns if '배당금' in c), None)
    if div_col:
        merged_df = merged_df.rename(columns={div_col: '보통주배당금(원)'})
        merged_df['보통주배당금_num'] = pd.to_numeric(merged_df['보통주배당금(원)'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        if '현재가' in merged_df.columns:
            merged_df['현재가_num'] = pd.to_numeric(merged_df['현재가'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
            merged_df['배당수익률'] = merged_df.apply(
                lambda x: (x['보통주배당금_num'] / x['현재가_num'] * 100) if x['현재가_num'] > 0 else 0, axis=1
            )
        else:
            merged_df['배당수익률'] = 0
    else:
        merged_df['보통주배당금(원)'] = 0
        merged_df['배당수익률'] = 0

    asset_col = next((c for c in merged_df.columns if '자산총계' in c), None)
    debt_col = next((c for c in merged_df.columns if '부채총계' in c), None)
    
    if asset_col and debt_col:
        merged_df['자산_num'] = pd.to_numeric(merged_df[asset_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        merged_df['부채_num'] = pd.to_numeric(merged_df[debt_col].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        merged_df['자본_num'] = merged_df['자산_num'] - merged_df['부채_num']
        
        merged_df['부채비율'] = merged_df.apply(
            lambda x: (x['부채_num'] / x['자본_num'] * 100) if x['자본_num'] > 0 else 0, axis=1
        )
    else:
        merged_df['부채비율'] = 0

    return merged_df

def merge_treasury_stock(df, csv_path='data.csv'):
    if os.path.exists(csv_path):
        print(f"[{csv_path}] 파일을 읽어 자사주 비율을 병합합니다.")
        try:
            csv_df = pd.read_csv(csv_path, encoding='cp949')
        except UnicodeDecodeError:
            try:
                csv_df = pd.read_csv(csv_path, encoding='euc-kr')
            except UnicodeDecodeError:
                csv_df = pd.read_csv(csv_path, encoding='utf-8')
        
        csv_df['종목코드'] = csv_df['종목코드'].astype(str).str.zfill(6)
        csv_df['자기주식수(D)'] = pd.to_numeric(csv_df['자기주식수(D)'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        csv_df['총발행주식수(C)'] = pd.to_numeric(csv_df['총발행주식수(C)'].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        
        csv_df['자사주 비율(%)'] = csv_df.apply(
            lambda x: (x['자기주식수(D)'] / x['총발행주식수(C)'] * 100) if x['총발행주식수(C)'] > 0 else 0, axis=1
        )
        df = pd.merge(df, csv_df[['종목코드', '자사주 비율(%)']], on='종목코드', how='left')
    else:
        print(f"\n※ 경고: {csv_path} 파일을 찾을 수 없어 자사주 비율이 빈값으로 처리됩니다.")
        df['자사주 비율(%)'] = None
        
    return df

def process_and_save_html(df, filename="index.html", name_max_width=90):
    print(f"모바일 앱 형태의 HTML 대시보드를 '{filename}'으로 생성 중입니다...")
    
    KST = timezone(timedelta(hours=9))
    update_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    
    # [수정] 상장주식수 제거. (종목코드는 링크 생성을 위해 일단 남겨둠)
    cols = ['종목명', '종목코드', '현재가', '전일비', '등락률', '기관 순매매량', '외국인 순매매량', '외국인 보유율(%)', 
            '시가총액', '매출액', '영업이익', '당기순이익', '부채비율', 'PER', 'PBR', '보통주배당금(원)', '배당수익률', '거래량', '자사주 비율(%)']
    
    df = df[[c for c in cols if c in df.columns]]
    
    def format_diff(row):
        try:
            raw_diff = str(row['전일비']).replace(',', '').replace('+', '').replace('-', '').strip()
            raw_diff = re.sub(r'[^\d]', '', raw_diff) 
            if not raw_diff: return str(row['전일비'])
            
            diff_val = int(raw_diff)
            if diff_val == 0: return '0'
            
            rate = float(str(row['등락률']).replace('%', '').replace(',', '').strip())
            
            if rate > 0:
                return f'<span style="color: #ff4d4d;">▲ {diff_val:,}</span>'
            elif rate < 0:
                return f'<span style="color: #4da6ff;">▼ {diff_val:,}</span>'
            else:
                return f'{diff_val:,}'
        except:
            return str(row['전일비'])

    def format_rate(row):
        try:
            rate = float(str(row['등락률']).replace('%', '').replace(',', '').strip())
            if rate > 0:
                return f'<span style="color: #ff4d4d;">+{rate:.2f}%</span>'
            elif rate < 0:
                return f'<span style="color: #4da6ff;">{rate:.2f}%</span>'
            else:
                return f'{rate:.2f}%'
        except:
            return str(row['등락률'])

    def format_net_buy(val):
        try:
            num = int(float(str(val).replace(',', '').replace('+', '').strip()))
            if num > 0:
                return f'<span style="color: #ff4d4d;">+{num:,}</span>'
            elif num < 0:
                return f'<span style="color: #4da6ff;">{num:,}</span>'
            else:
                return '0'
        except:
            return str(val)

    df['전일비'] = df.apply(format_diff, axis=1)
    df['등락률'] = df.apply(format_rate, axis=1)
    
    for col in ['기관 순매매량', '외국인 순매매량']:
        if col in df.columns:
            df[col] = df[col].apply(format_net_buy)

    # [수정] 상장주식수 제외 적용
    int_cols = ['현재가', '보통주배당금(원)', '시가총액', '매출액', '영업이익', '당기순이익', '거래량']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
            df[col] = df[col].apply(lambda x: f"{x:,}" if x != 0 else '-')
            
    float_cols = ['부채비율', '외국인 보유율(%)', 'PER', 'PBR', '배당수익률', '자사주 비율(%)']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace('%', '').str.replace(',', ''), errors='coerce')
            df[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '-')

    if '종목명' in df.columns and '종목코드' in df.columns:
        df['종목명'] = df.apply(
            lambda row: f'<a href="https://finance.naver.com/item/main.naver?code={row["종목코드"]}" target="_blank" class="text-info text-decoration-none fw-bold" style="display: inline-block; max-width: {name_max_width}px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: middle;">{row["종목명"]}</a>', axis=1
        )

    # [핵심] HTML 테이블 생성 직전에 '종목코드' 컬럼을 완전히 삭제하여 화면에서 숨김
    df = df.drop(columns=['종목코드'], errors='ignore')

    html_table = df.to_html(classes='table table-dark table-striped table-hover align-middle nowrap', table_id='stockTable', index=False, escape=False)
    td_max_width = name_max_width + 5

    html_template = f"""
    <!DOCTYPE html>
    <html data-bs-theme="dark">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
        
        <meta name="apple-mobile-web-app-capable" content="yes">
        <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent">
        <meta name="apple-mobile-web-app-title" content="국내증시">
        <meta name="mobile-web-app-capable" content="yes">
        <meta name="theme-color" content="#121212">
        <link rel="apple-touch-icon" href="https://cdn-icons-png.flaticon.com/512/2942/2942244.png">
        <link rel="shortcut icon" href="https://cdn-icons-png.flaticon.com/512/2942/2942244.png">
        
        <title>국내 증시 대시보드 앱</title>
        
        <link href="https://cdnjs.cloudflare.com/ajax/libs/twitter-bootstrap/5.3.0/css/bootstrap.min.css" rel="stylesheet">
        <link href="https://cdn.datatables.net/1.13.6/css/dataTables.bootstrap5.min.css" rel="stylesheet">
        <link href="https://cdn.datatables.net/fixedheader/3.4.0/css/fixedHeader.bootstrap5.min.css" rel="stylesheet">
        <link href="https://cdn.datatables.net/fixedcolumns/4.3.0/css/fixedColumns.bootstrap5.min.css" rel="stylesheet">
        
        <style>
            body {{ padding: 15px; background-color: #121212; font-size: 0.85rem; }}
            .container-fluid {{ background-color: #1e1e1e; padding: 15px; border-radius: 8px; box-shadow: 0 4px 10px rgba(0,0,0,0.5); }}
            
            .header-container {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }}
            h2 {{ color: #ffffff; font-size: 1.5rem; margin: 0; }}
            
            #stockTable th {{ background-color: #2c2c2c; color: #e0e0e0; text-align: center; vertical-align: middle; white-space: nowrap; }}
            #stockTable td {{ text-align: right; white-space: nowrap; border-color: #333; }}
            
            #stockTable td:nth-child(1) {{ text-align: left; max-width: {td_max_width}px; }}
            #stockTable td:nth-child(2) {{ text-align: center; }}
            td:contains('-') {{ color: #777; }}
            
            th.dtfc-fixed-left, td.dtfc-fixed-left {{ background-color: #1e1e1e !important; z-index: 1; text-align: left !important; }}
            thead th.dtfc-fixed-left {{ background-color: #2c2c2c !important; z-index: 2; border-bottom: 1px solid #444; }}

            @media (max-width: 768px) {{
                body {{ padding: 5px; font-size: 0.75rem; }}
                .container-fluid {{ padding: 10px; }}
                h2 {{ font-size: 1.1rem; }}
                .btn-sm {{ font-size: 0.75rem; padding: 0.25rem 0.5rem; }}
                .alert {{ font-size: 0.75rem; padding: 8px; margin-bottom: 10px; }}
                .dataTables_filter input {{ max-width: 130px; }}
            }}
        </style>
    </head>
    <body>
        <div class="container-fluid">
            <div class="header-container">
                <h2 class="fw-bold">국내 주식 대시보드</h2>
                <button id="resetBtn" class="btn btn-outline-light btn-sm">🔄 초기화</button>
            </div>
            
            <div class="alert alert-secondary text-center border-secondary text-light bg-dark">
                <span class="badge bg-primary mb-2" style="font-size: 0.85rem;">⏱ 업데이트: {update_time_str}</span><br>
                ※ 📱 <strong>모바일 앱 모드:</strong> 좌측 '종목명' 고정, 좌우 스와이프 지원.<br>
                ※ 상승/매수는 <strong><span style="color: #ff4d4d;">▲빨강</span></strong>, 하락/매도는 <strong><span style="color: #4da6ff;">▼파랑</span></strong> 기호와 함께 직관적으로 표기됩니다.
            </div>
            {html_table}
        </div>

        <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/dataTables.bootstrap5.min.js"></script>
        <script src="https://cdn.datatables.net/fixedheader/3.4.0/js/dataTables.fixedHeader.min.js"></script>
        <script src="https://cdn.datatables.net/fixedcolumns/4.3.0/js/dataTables.fixedColumns.min.js"></script>

        <script>
            $(document).ready( function () {{
                var table = $('#stockTable').DataTable({{
                    "paging": false,
                    "scrollY": "70vh",
                    "scrollX": true,
                    "scrollCollapse": true,
                    "fixedHeader": true,
                    "fixedColumns": {{
                        "leftColumns": 1
                    }},
                    "searching": true,
                    "ordering": true,
                    // [수정] 종목코드 컬럼이 빠지면서 시가총액 위치가 7번 인덱스로 당겨짐
                    "order": [[ 7, "desc" ]], 
                    "language": {{ "url": "//cdn.datatables.net/plug-ins/1.13.6/i18n/ko.json" }}
                }});

                $('#resetBtn').on('click', function() {{
                    table.search('').columns().search('');
                    // [수정] 초기화 시 정렬도 7번(시가총액)으로 복원
                    table.order([[ 7, "desc" ]]);
                    table.draw();
                }});
            }});
        </script>
    </body>
    </html>
    """
    
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html_template)
    print(f"\n[성공] 최종 통합 대시보드가 '{filename}' 이름으로 생성되었습니다. (업데이트 시간: {update_time_str})")

if __name__ == "__main__":
    df = get_full_market_data()
    df = merge_treasury_stock(df, 'data.csv')
    process_and_save_html(df, filename="index.html", name_max_width=90)
