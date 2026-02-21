import requests
import pandas as pd
from bs4 import BeautifulSoup
from io import StringIO
from tqdm import tqdm
import os
# [추가된 부분] 시간 계산을 위한 모듈 임포트
from datetime import datetime, timedelta, timezone

def set_naver_custom_fields(session, field_ids):
    """네이버 증권 시가총액 페이지의 추가 컬럼을 설정"""
    url = "https://finance.naver.com/sise/field_submit.naver"
    params = [('menu', 'market_sum'), ('returnUrl', 'http://finance.naver.com/sise/sise_market_sum.naver')]
    for fid in field_ids:
        params.append(('fieldIds', fid))
    session.get(url, params=params)

def crawl_market_sum(session, desc_label):
    """설정된 컬럼 기반으로 코스피/코스닥 전 페이지를 크롤링"""
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

def get_full_market_data():
    """2-Pass 크롤링 및 부채비율 직접 계산 로직"""
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    group1 = ['sales', 'operating_profit', 'net_income', 'property_total', 'debt_total', 'foreign_rate']
    set_naver_custom_fields(session, group1)
    df1 = crawl_market_sum(session, "1차 데이터 수집")
    
    group2 = ['market_sum', 'per', 'pbr', 'quant', 'listed_stock_cnt']
    set_naver_custom_fields(session, group2)
    df2 = crawl_market_sum(session, "2차 데이터 수집")
    
    common_cols = ['종목코드', '종목명']
    merged_df = pd.merge(df1, df2.drop(columns=['현재가', '전일비', '등락률'], errors='ignore'), on=common_cols, how='left')
    
    print("\n수집된 데이터를 바탕으로 부채비율을 계산합니다...")
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
    """data.csv를 읽어 자사주 비율(%) 계산 및 병합"""
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
    """최종 대시보드 HTML 파일 생성 (모바일, PWA, 초기화 버튼 포함)"""
    print(f"모바일 앱 형태의 HTML 대시보드를 '{filename}'으로 생성 중입니다...")
    
    # [추가된 부분] HTML을 생성하는 시점의 한국 시간(KST) 구하기
    KST = timezone(timedelta(hours=9))
    update_time_str = datetime.now(KST).strftime("%Y-%m-%d %H:%M")
    
    if '상장주식수' in df.columns:
        df = df.rename(columns={'상장주식수': '상장주식수(천주)'})
    
    cols = ['종목명', '종목코드', '현재가', '전일비', '등락률', '시가총액', '매출액', '영업이익', 
            '당기순이익', '부채비율', '외국인비율', 'PER', 'PBR', '거래량', '상장주식수(천주)', '자사주 비율(%)']
    
    df = df[[c for c in cols if c in df.columns]]
    
    int_cols = ['현재가', '시가총액', '매출액', '영업이익', '당기순이익', '거래량', '상장주식수(천주)']
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', ''), errors='coerce').fillna(0).astype(int)
            df[col] = df[col].apply(lambda x: f"{x:,}" if x != 0 else '-')
            
    float_cols = ['등락률', '부채비율', '외국인비율', 'PER', 'PBR', '자사주 비율(%)']
    for col in float_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].apply(lambda x: f"{x:.2f}" if pd.notna(x) else '-')

    if '종목명' in df.columns and '종목코드' in df.columns:
        df['종목명'] = df.apply(
            lambda row: f'<a href="https://finance.naver.com/item/main.naver?code={row["종목코드"]}" target="_blank" class="text-info text-decoration-none fw-bold" style="display: inline-block; max-width: {name_max_width}px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; vertical-align: middle;">{row["종목명"]}</a>', axis=1
        )

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
                    "order": [[ 5, "desc" ]], // 기본 정렬: 시가총액 내림차순
                    "language": {{ "url": "//cdn.datatables.net/plug-ins/1.13.6/i18n/ko.json" }}
                }});

                // 초기화 버튼 이벤트
                $('#resetBtn').on('click', function() {{
                    table.search('').columns().search('');
                    table.order([[ 5, "desc" ]]);
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

# ================= 실행부 =================
if __name__ == "__main__":
    df = get_full_market_data()
    df = merge_treasury_stock(df, 'data.csv')

    process_and_save_html(df, filename="index.html", name_max_width=90)
