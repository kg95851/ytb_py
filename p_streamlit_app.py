import streamlit as st
import os
import re
import time
# import threading # 백그라운드 실행 제거
import queue
import random
import hashlib
import logging
from datetime import datetime, timezone, timedelta
from urllib.parse import quote
import requests
import html
import json
import pandas as pd
import io

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from twocaptcha import TwoCaptcha
from PIL import Image
import base64

# APP_DATA_FILE = "app_data.json" # 로컬 파일 저장 기능 삭제

# --- Page Config ---
st.set_page_config(
    page_title="Playboard Scraper",
    page_icon="📊",
    layout="wide"
)

# --- Data Persistence Functions (삭제) ---
# save_app_data, load_app_data 함수를 모두 삭제합니다.

# --- Session State Initialization ---
def initialize_session_state():
    """Initializes all the necessary variables in streamlit's session state for each user."""
    # persistent_data = load_app_data() # 로컬 파일 로딩 로직 삭제
    
    if 'driver' not in st.session_state:
        st.session_state.driver = None
    if 'login_status' not in st.session_state:
        st.session_state.login_status = "Not logged in"
    if 'log_messages' not in st.session_state:
        st.session_state.log_messages = []
    if 'scraped_data' not in st.session_state:
        st.session_state.scraped_data = pd.DataFrame()
    if 'is_scraping' not in st.session_state:
        st.session_state.is_scraping = False
    if 'progress' not in st.session_state:
        st.session_state.progress = 0
    if 'subscriber_filters' not in st.session_state:
        st.session_state.subscriber_filters = {
            "0K~100K": {"0~1K": False, "1K~5K": False, "5K~10K": False, "10K~50K": False, "50K~100K": False},
            "100K~1M": {"100K~500K": False, "500K~1M": False},
            "1M~5M": {"1M~1.5M": False, "1.5M~2M": False, "2M~2.5M": False, "2.5M~3M": False, "3M~3.5M": False, "3.5M~4M": False, "4M~4.5M": False, "4.5M~5M": False},
            "5M~10M": {"5M~5.5M": False, "5.5M~6M": False, "6M~6.5M": False, "6.5M~7M": False, "7M~7.5M": False, "7.5M~8M": False, "8M~8.5M": False, "8.5M~9M": False, "9M~9.5M": False, "9.5M~10M": False},
            "10M+": {"10M+": False}
        }
        st.session_state.filter_ranges = {
            "0~1K": (0, 1000), "1K~5K": (1000, 5000), "5K~10K": (5000, 10000), "10K~50K": (10000, 50000), "50K~100K": (50000, 100000),
            "100K~500K": (100000, 500000), "500K~1M": (500000, 1000000),
            "1M~1.5M": (1000000, 1500000), "1.5M~2M": (1500000, 2000000), "2M~2.5M": (2000000, 2500000), "2.5M~3M": (2500000, 3000000), "3M~3.5M": (3000000, 3500000), "3.5M~4M": (3500000, 4000000), "4M~4.5M": (4000000, 4500000), "4.5M~5M": (4500000, 5000000),
            "5M~10M": {"5M~5.5M": (5000000, 5500000), "5.5M~6M": (5500000, 6000000), "6M~6.5M": (6000000, 6500000), "6.5M~7M": (6500000, 7000000), "7M~7.5M": (7000000, 7500000), "7.5M~8M": (7500000, 8000000), "8M~8.5M": (8000000, 8500000), "8.5M~9M": (8500000, 9000000), "9M~9.5M": (9000000, 9500000), "9.5M~10M": (9500000, 10000000)},
            "10M+": {"10M+": (10000000, float('inf'))}
        }
    if 'use_custom_filter' not in st.session_state:
        st.session_state.use_custom_filter = False
    if 'custom_min' not in st.session_state:
        st.session_state.custom_min = -1
    if 'custom_max' not in st.session_state:
        st.session_state.custom_max = -1
    if 'is_filter_applied' not in st.session_state:
        st.session_state.is_filter_applied = False
    
    if 'crawl_settings' not in st.session_state:
        st.session_state.crawl_settings = {
            "max_items": 5000, "dates": [], "country_code": "south-korea", "country_name": "한국"
        }
    
    if 'shopping_cart' not in st.session_state:
        # cart_data = persistent_data.get('shopping_cart', []) # 삭제
        st.session_state.shopping_cart = pd.DataFrame()

    if 'custom_groups' not in st.session_state:
        # groups_data = persistent_data.get('custom_groups', {}) # 삭제
        st.session_state.custom_groups = {}

    if '2captcha_api_key' not in st.session_state:
        # st.session_state['2captcha_api_key'] = persistent_data.get('2captcha_api_key', '') # 삭제
        st.session_state['2captcha_api_key'] = ''

initialize_session_state()

# --- Logging and Progress ---
def log(message):
    timestamp = datetime.now().strftime("%H:%M:%S")
    st.session_state.log_messages.append(f"[{timestamp}] {message}")
    print(f"[LOG] {message}")

def update_progress(value):
    st.session_state.progress = value

# --- Helper Functions ---
def extract_video_id_from_thumbnail(thumb_url):
    if "ytimg.com/vi/" in thumb_url:
        parts = thumb_url.split("ytimg.com/vi/")
        if len(parts) > 1:
            video_id = parts[1].split("/")[0].split("_")[0]
            if 8 <= len(video_id) <= 15:
                return video_id
    return None

def extract_video_id_from_href(href):
    if "/video/" in href:
        parts = href.split("/video/")
        if len(parts) > 1:
            video_id = parts[1].split("?")[0].split("&")[0]
            if 8 <= len(video_id) <= 15:
                return video_id
    return None

def parse_subscriber_count(count_text):
    if not count_text or count_text == "구독자 정보 없음":
        return count_text
    return re.sub(r'[^0-9,]', '', count_text)

def convert_subscriber_count_to_int(count_text):
    if not count_text or count_text == "구독자 정보 없음":
        return -1
    try:
        return int(count_text.replace(',', ''))
    except (ValueError, TypeError):
        return -1

def parse_views_to_int(view_text):
    """Converts view counts with suffixes (K, M, B) to integers."""
    view_text = str(view_text).strip().upper()
    view_text = view_text.replace(',', '')
    if 'K' in view_text:
        return int(float(view_text.replace('K', '')) * 1000)
    if 'M' in view_text:
        return int(float(view_text.replace('M', '')) * 1000000)
    if 'B' in view_text:
        return int(float(view_text.replace('B', '')) * 1000000000)
    try:
        return int(view_text)
    except (ValueError, TypeError):
        return 0

def generate_hash(title, channel):
    normalized_title = re.sub(r'[^\w]', '', title.lower())
    normalized_channel = channel.lower()
    combined = f"{normalized_title}|{normalized_channel}"
    return hashlib.md5(combined.encode()).hexdigest()

@st.cache_data
def convert_df_to_csv(df):
    return df.to_csv(index=False).encode('utf-8')

def parse_dates(date_str):
    dates = []
    parts = date_str.replace(" ", "").split(',')
    for part in parts:
        if '-' in part:
            try:
                start_s, end_s = part.split('-')
                start_d = datetime.strptime(start_s, '%Y%m%d')
                end_d = datetime.strptime(end_s, '%Y%m%d')
                delta = end_d - start_d
                for i in range(delta.days + 1):
                    day = start_d + timedelta(days=i)
                    dates.append(day.strftime('%Y%m%d'))
            except Exception as e:
                log(f"날짜 범위 파싱 오류 '{part}': {e}")
        elif len(part) == 8 and part.isdigit():
            dates.append(part)
    return sorted(list(set(dates)))

def convert_df_to_pdf(df):
    try:
        plt.rcParams['font.family'] = 'Malgun Gothic'
        plt.rcParams['axes.unicode_minus'] = False 
    except Exception as e:
        log("⚠️ 'Malgun Gothic' 폰트를 찾을 수 없습니다. PDF의 한글이 깨질 수 있습니다.")
        log("Windows 사용자가 아닌 경우, 시스템에 맞는 한글 폰트를 설치하고 코드를 수정해야 합니다.")

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.axis('tight')
    ax.axis('off')
    
    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center', colWidths=[0.1, 0.3, 0.1, 0.15, 0.1, 0.15, 0.1, 0.2])
    table.auto_set_font_size(False)
    table.set_fontsize(8)
    table.scale(1.2, 1.2)

    pdf_buffer = io.BytesIO()
    with PdfPages(pdf_buffer) as pdf:
        pdf.savefig(fig, bbox_inches='tight')
    
    return pdf_buffer.getvalue()


# --- Filter Logic ---
def should_include_subscriber(subscriber_count):
    if not st.session_state.is_filter_applied:
        return True
    if subscriber_count == -1:
        return False

    any_filter_selected = any(
        st.session_state.subscriber_filters[cat][range_name]
        for cat in st.session_state.subscriber_filters
        for range_name in st.session_state.subscriber_filters[cat]
    )

    if not any_filter_selected and not st.session_state.use_custom_filter:
        return True

    for cat in st.session_state.subscriber_filters:
        for range_name, is_selected in st.session_state.subscriber_filters[cat].items():
            if is_selected:
                min_val, max_val = st.session_state.filter_ranges[range_name]
                if min_val <= subscriber_count < max_val:
                    return True
    
    if st.session_state.use_custom_filter:
        min_val, max_val = st.session_state.custom_min, st.session_state.custom_max
        if min_val >= 0 and max_val < 0:
            return subscriber_count >= min_val
        elif min_val < 0 and max_val >= 0:
            return subscriber_count <= max_val
        elif min_val >= 0 and max_val >= 0:
            return min_val <= subscriber_count <= max_val
            
    return False

# --- Selenium/Scraping Logic ---
def init_driver():
    options = Options()
    # 사용자가 선택한 모드에 따라 헤드리스 옵션을 조건부로 추가합니다.
    if st.session_state.get("run_headless", True):
        log("INFO: 헤드리스 모드로 실행합니다.")
        options.add_argument("--headless")
        options.add_argument("--window-size=1920,1080")
    else:
        log("INFO: 일반 모드(헤드리스 아님)로 실행합니다. 캡챠 발생 시 직접 해결할 수 있습니다.")

    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    
    try:
        log("INFO: WebDriver를 초기화합니다.")
        driver = webdriver.Chrome(options=options)
        driver.set_page_load_timeout(30)
        return driver
    except Exception as e:
        log(f"❌ WebDriver 초기화 실패: {e}")
        st.error(f"WebDriver 초기화 실패: {e}. 배포 환경 설정을 확인하세요.")
        return None

def do_login(email, password):
    if st.session_state.driver is None:
        st.session_state.driver = init_driver()
        if st.session_state.driver is None:
            return

    driver = st.session_state.driver
    log("🌍 사이트 접속 중...")
    try:
        driver.get("https://playboard.co")
        wait = WebDriverWait(driver, 15)
        
        login_link = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[text()='로그인']")))
        login_link.click()

        email_input = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@name='email']")))
        password_input = wait.until(EC.presence_of_element_located((By.XPATH, "//input[@name='password']")))
        email_input.send_keys(email)
        password_input.send_keys(password)

        login_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//button[@type='submit' and .//span[text()='로그인']]")))
        login_button.click()

        wait.until(EC.invisibility_of_element_located((By.XPATH, "//input[@name='email']")))
        st.session_state.login_status = "✅ 로그인 성공!"
        log("✅ 로그인 성공!")
    except Exception as e:
        st.session_state.login_status = f"❌ 로그인 실패: {e}"
        log(st.session_state.login_status)
        if st.session_state.driver:
            st.session_state.driver.quit()
            st.session_state.driver = None

def detect_and_handle_captcha(driver):
    try:
        # 1. 메인 프레임에서 'reCAPTCHA' iframe 찾기
        recaptcha_iframe = driver.find_elements(By.CSS_SELECTOR, "iframe[title='reCAPTCHA']")
        if not recaptcha_iframe:
            return None # 캡챠 없음

        log("INFO: '로봇이 아닙니다' 캡챠 iframe 발견.")
        driver.switch_to.frame(recaptcha_iframe[0])
        
        # 2. '로봇이 아닙니다' 체크박스 클릭
        checkbox = driver.find_elements(By.ID, "recaptcha-anchor")
        if checkbox:
            checkbox[0].click()
            log("INFO: '로봇이 아닙니다' 체크박스를 클릭했습니다.")
            time.sleep(3) # 이미지 챌린지가 로드될 시간을 줍니다.
        
        # 3. 기본 컨텐츠로 돌아와서, 이미지 챌린지 iframe 찾기
        driver.switch_to.default_content()
        challenge_iframe = driver.find_elements(By.CSS_SELECTOR, "iframe[title*='보안문자']")
        
        if not challenge_iframe:
            log("INFO: 이미지 챌린지가 나타나지 않았습니다. 캡챠가 해결된 것 같습니다.")
            return True

        if st.session_state.get('2captcha_api_key'):
            log("🚨 이미지 선택형 캡챠 발견! 2Captcha로 자동 해결을 시도합니다.")
            driver.switch_to.frame(challenge_iframe[0])

            # 4. 이미지 캡처 및 질문 추출
            img_element = driver.find_element(By.CSS_SELECTOR, "img.rc-image-tile-44")
            img_base64 = img_element.screenshot_as_base64
            
            instruction_element = driver.find_element(By.CSS_SELECTOR, ".rc-imageselect-instructions strong")
            instruction_text = instruction_element.text
            log(f"캡챠 질문: '{instruction_text}'에 해당하는 이미지를 선택합니다.")

            # 5. 2Captcha로 해결 요청
            config = {'apiKey': st.session_state['2captcha_api_key']}
            solver = TwoCaptcha(**config)
            try:
                result = solver.recaptcha(
                    sitekey=driver.find_element(By.CSS_SELECTOR, ".g-recaptcha").get_attribute('data-sitekey'), # 사이트 키를 다시 가져옴
                    url=driver.current_url,
                    method='image',
                    textinstructions=instruction_text,
                    body=f'base64:{img_base64}'
                )
                
                log("✅ 2Captcha 이미지 분석 완료. 토큰을 주입합니다.")
                recaptcha_response = result['code']
                
                driver.switch_to.default_content() # 토큰 주입을 위해 기본 프레임으로 돌아감
                driver.execute_script(f"document.getElementById('g-recaptcha-response').innerHTML = '{recaptcha_response}';")
                log("INFO: 캡챠 해결 토큰 주입 완료.")
                
                # 콜백 함수 실행 또는 제출 시도
                driver.switch_to.frame(challenge_iframe[0]) # 다시 챌린지 프레임으로
                verify_button = driver.find_element(By.ID, "recaptcha-verify-button")
                verify_button.click()
                log("INFO: 확인 버튼 클릭.")
                
                driver.switch_to.default_content()
                time.sleep(5)
                return True

            except Exception as e:
                log(f"❌ 2Captcha 이미지 해결 실패: {e}")
                driver.switch_to.default_content()
                return False
        
        elif challenge_iframe:
             log("이미지 캡챠가 감지되었으나, 2Captcha API 키가 없습니다. 헤드리스 모드를 끄고 직접 해결해주세요.")
             driver.switch_to.default_content()
             return False

    except Exception as e:
        log(f"⚠️ 캡챠 감지/처리 중 예상치 못한 오류: {e}")
        try:
            driver.switch_to.default_content()
        except: pass
    return False

def crawl(is_short, dates, country_code, country_name, max_items):
    st.session_state.is_scraping = True
    st.session_state.progress = 0

    try:
        all_collected_data = []
        processed_hashes = set()

        driver = st.session_state.driver
        if not driver:
            log("❌ 드라이버가 없습니다. 먼저 로그인 해주세요.")
            return

        for a_date in dates:
            try:
                date_obj = datetime.strptime(a_date, '%Y%m%d')
                kst = timezone(timedelta(hours=9))
                date_obj = date_obj.replace(tzinfo=kst)
                key = int(date_obj.timestamp())
            except ValueError:
                log(f"❌ 날짜 형식이 올바르지 않습니다: {a_date}")
                continue

            log(f"🎯 {date_obj.strftime('%Y-%m-%d')} 날짜 크롤링 시작 (목표: {max_items}개)...")

            if is_short:
                url = f"https://playboard.co/chart/short/most-viewed-all-videos-in-{country_code}-daily?period={key}"
            else:
                url = f"https://playboard.co/chart/video/?period={key}"
            
            log(f"➡️ URL로 이동 중: {url}")
            
            try:
                driver.get(url)
                log("...페이지 로딩 대기 중...")
                WebDriverWait(driver, 45).until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "a.title__label")))
                log("✅ 페이지 로딩 완료.")
            except TimeoutException:
                log(f"❌ 페이지 로딩 시간 초과: {url}")
                log("네트워크 연결을 확인하거나, 사이트가 다운되었을 수 있습니다. 다음 날짜로 넘어갑니다.")
                continue
            except Exception as e:
                log(f"❌ 페이지 로딩 실패: {e}")
                continue

            scroll_count = 0
            max_scrolls = (max_items // 20) + 15
            no_change_count = 0
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            prev_items_count = 0
            while True:
                current_items_on_page = len(driver.find_elements(By.CSS_SELECTOR, "a.title__label"))
                status_text.text(f"스크롤 {scroll_count}회, 페이지 항목: {current_items_on_page}개")
                
                if current_items_on_page >= max_items:
                    log(f"✅ 목표 항목 수({max_items}) 이상을 페이지에서 찾았습니다. 스크롤을 중단합니다.")
                    break

                if scroll_count >= max_scrolls:
                    log(f"⚠️ 최대 스크롤 횟수({max_scrolls})에 도달했습니다. 현재까지 찾은 항목으로 진행합니다.")
                    break
                
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                scroll_count += 1
                time.sleep(2.5) 
                
                new_items_on_page = len(driver.find_elements(By.CSS_SELECTOR, "a.title__label"))

                if new_items_on_page == prev_items_count:
                    no_change_count += 1
                    log(f"⚠️ 스크롤 후 새 항목이 로드되지 않았습니다. ({no_change_count}/3)")
                    if no_change_count >= 3:
                        log("더 이상 새 항목이 로드되지 않아 스크롤을 중단합니다.")
                        if not detect_and_handle_captcha(driver):
                            break
                        else: 
                            no_change_count = 0
                else:
                    no_change_count = 0
                prev_items_count = new_items_on_page

            log("🔍 데이터 수집 및 처리 중...")
            
            title_elements = driver.find_elements(By.CSS_SELECTOR, "a.title__label")
            view_elements = driver.find_elements(By.CSS_SELECTOR, "span.fluc-label")
            thumbnail_elements = driver.find_elements(By.CSS_SELECTOR, "div.thumb-wrapper.image div.thumb.lazy-image")
            channel_elements = driver.find_elements(By.CSS_SELECTOR, "td.channel a span.name")
            subscriber_elements = driver.find_elements(By.CSS_SELECTOR, "div.subs span.subs__count")

            log(f"총 {len(title_elements)}개 항목을 페이지에서 발견하여 처리를 시작합니다.")
            
            for i in range(len(title_elements)):
                if len(all_collected_data) >= max_items:
                    log(f"목표 수집량({max_items}개)에 도달하여 수집을 중단합니다.")
                    break 
                try:
                    title = title_elements[i].text.strip()
                    channel = channel_elements[i].text.strip() if i < len(channel_elements) else "N/A"
                    
                    item_hash = generate_hash(title, channel)
                    if item_hash in processed_hashes:
                        continue

                    views = view_elements[i].text.strip() if i < len(view_elements) else "N/A"
                    subscriber_count_text = subscriber_elements[i].text.strip() if i < len(subscriber_elements) else "구독자 정보 없음"
                    
                    subscriber_count_int = convert_subscriber_count_to_int(subscriber_count_text)
                    views_numeric = parse_views_to_int(views)

                    if not should_include_subscriber(subscriber_count_int):
                        continue

                    thumb_url = ""
                    if i < len(thumbnail_elements):
                        thumb_url_raw = thumbnail_elements[i].get_attribute("data-background-image")
                        if thumb_url_raw and thumb_url_raw.startswith("//"):
                            thumb_url = "https:" + thumb_url_raw

                    video_href = title_elements[i].get_attribute("href")
                    video_id = extract_video_id_from_href(video_href) or extract_video_id_from_thumbnail(thumb_url)
                    youtube_url = f"https://www.youtube.com/watch?v={video_id}" if video_id else ""

                    all_collected_data.append({
                        'Thumbnail': thumb_url,
                        'Title': title,
                        'Views': views,
                        'Views_numeric': views_numeric, # Add numeric views
                        'Channel': channel,
                        'Date': date_obj.strftime('%Y-%m-%d'),
                        'Subscribers': subscriber_count_text,
                        'Subscribers_numeric': subscriber_count_int, # Add numeric subscribers
                        'Hash': item_hash,
                        'YouTube URL': youtube_url
                    })
                    processed_hashes.add(item_hash)
                    
                    current_progress = int((len(all_collected_data) / max_items) * 100)
                    progress_bar.progress(min(current_progress, 100))

                except Exception as e:
                    log(f"⚠️ 항목 {i+1} 처리 중 오류 발생: {e}")
                    continue 

        new_df = pd.DataFrame(all_collected_data)
        if not new_df.empty:
            st.session_state.scraped_data = pd.concat([st.session_state.scraped_data, new_df]).drop_duplicates(subset=['Hash']).reset_index(drop=True)
        
        log(f"✅ 크롤링 완료! 총 {len(new_df)}개 신규 항목 발견, 현재 총 {len(st.session_state.scraped_data)}개 결과.")

    except Exception as e:
        log(f"❌ 크롤링 중 심각한 오류 발생: {e}")
    finally:
        st.session_state.is_scraping = False
        st.session_state.progress = 0


# --- Streamlit UI ---
st.title("📊 Playboard Scraper")

# --- Sidebar for Inputs ---
with st.sidebar:
    st.header("⚙️ 설정")

    with st.expander("🔑 로그인 및 API 설정", expanded=True):
        email = st.text_input("Playboard 이메일")
        password = st.text_input("Playboard 비밀번호", type="password")
        
        st.session_state['2captcha_api_key'] = st.text_input(
            "2Captcha API 키 (선택 사항)", 
            value=st.session_state.get('2captcha_api_key', ''),
            type="password",
            help="캡챠 자동 해결을 위해 2Captcha API 키를 입력하세요. 브라우저 탭을 닫으면 초기화됩니다."
        )

        st.checkbox("헤드리스 모드로 실행", value=True, key="run_headless", help="체크 해제 시 크롬 창이 나타나며, 캡챠를 직접 해결할 수 있습니다.")

        if st.button("로그인", disabled=st.session_state.driver is not None):
            if email and password:
                with st.spinner("로그인 중..."):
                    do_login(email, password)
                    st.rerun()
            else:
                st.warning("이메일과 비밀번호를 입력해주세요.")
        
        st.info(st.session_state.login_status)
        if st.session_state.driver is not None:
            if st.button("로그아웃/드라이버 종료"):
                st.session_state.driver.quit()
                st.session_state.driver = None
                st.session_state.login_status = "Not logged in"
                st.rerun()

    with st.expander("📝 크롤링 설정", expanded=True):
        max_items_to_crawl = st.selectbox(
            "최대 수집 순위",
            (200, 500, 2500, 5000),
            index=3,
            key="max_items_selector"
        )
        
        date_input_text = st.text_area(
            "날짜 입력 (YYYYMMDD 형식)",
            help="하나의 날짜 (20240101), 여러 날짜 (20240101, 20240102), 또는 범위 (20240101-20240105)를 입력하세요.",
            key="date_input"
        )

        country_option = st.radio(
            "국가 (숏폼 전용)",
            ('한국', '미국', '일본'),
            horizontal=True,
            key="country_selector"
        )

        if st.button("설정 완료"):
            country_map = {
                '한국': ('south-korea', '한국'),
                '미국': ('united-states', '미국'),
                '일본': ('japan', '일본')
            }
            parsed_dates = parse_dates(st.session_state.date_input)
            if parsed_dates:
                st.session_state.crawl_settings = {
                    "max_items": st.session_state.max_items_selector,
                    "dates": parsed_dates,
                    "country_code": country_map[st.session_state.country_selector][0],
                    "country_name": country_map[st.session_state.country_selector][1]
                }
                st.success("설정이 저장되었습니다!")
                log(f"설정 저장됨: {st.session_state.crawl_settings['max_items']}개, 날짜: {st.session_state.crawl_settings['dates']}, 국가: {st.session_state.crawl_settings['country_name']}")
            else:
                st.error("유효한 날짜를 입력한 후 설정을 완료해주세요.")

    with st.expander("📊 구독자 수 필터"):
        st.session_state.is_filter_applied = st.checkbox("필터 적용하기")
        
        for category, ranges in st.session_state.subscriber_filters.items():
            with st.container():
                st.write(f"**{category}**")
                cols = st.columns(3)
                i = 0
                for range_name in ranges:
                    st.session_state.subscriber_filters[category][range_name] = cols[i % 3].checkbox(range_name, value=st.session_state.subscriber_filters[category][range_name], key=f"filter_{category}_{range_name}")
                    i += 1
        
        st.write("**커스텀 범위**")
        st.session_state.use_custom_filter = st.checkbox("커스텀 필터 사용")
        col1, col2 = st.columns(2)
        custom_min_input = col1.text_input("최소 구독자 수", placeholder="예: 12000")
        custom_max_input = col2.text_input("최대 구독자 수", placeholder="예: 55000")
        
        try:
            st.session_state.custom_min = int(custom_min_input.replace(',', '')) if custom_min_input else -1
            st.session_state.custom_max = int(custom_max_input.replace(',', '')) if custom_max_input else -1
        except ValueError:
            st.error("구독자 수는 숫자로 입력해야 합니다.")


# --- Main Area ---
if st.button("🚀 숏폼 크롤링 시작", disabled=(st.session_state.is_scraping or st.session_state.driver is None or not st.session_state.crawl_settings['dates']), use_container_width=True):
    # st.session_state.scraped_data = pd.DataFrame() # 더 이상 초기화하지 않음
    settings = st.session_state.crawl_settings
    with st.spinner(f"{settings['max_items']}개 숏폼 크롤링 진행 중..."):
        crawl(True, settings['dates'], settings['country_code'], settings['country_name'], settings['max_items'])
    st.rerun()

if st.button("🎬 롱폼 크롤링 시작", disabled=(st.session_state.is_scraping or st.session_state.driver is None or not st.session_state.crawl_settings['dates']), use_container_width=True):
    # st.session_state.scraped_data = pd.DataFrame() # 더 이상 초기화하지 않음
    settings = st.session_state.crawl_settings
    with st.spinner(f"{settings['max_items']}개 롱폼 크롤링 진행 중..."):
        crawl(False, settings['dates'], 'south-korea', '한국', settings['max_items'])
    st.rerun()

# --- Logging and Progress Display ---
st.text_area("Logs", "\n".join(st.session_state.log_messages), height=300, key="log_area_final")

# --- Results Display ---
tab1, tab2 = st.tabs(["📊 크롤링 결과", "📺 유튜브 결과 (현재 세션)"])

with tab1:
    st.header("📊 크롤링 결과")
    if not st.session_state.scraped_data.empty:
        # --- Sorting and Controls ---
        sort_option = st.selectbox(
            "결과 정렬",
            options=["기본", "채널별 정렬", "조회수 높은 순", "조회수 낮은 순", "구독자 많은 순", "구독자 적은 순"],
            key="sort_scraped"
        )
        if st.button("크롤링 결과 초기화", use_container_width=True):
            st.session_state.scraped_data = pd.DataFrame()
            st.rerun()

        # --- Data Sorting Logic ---
        display_df = st.session_state.scraped_data.copy()
        if sort_option == "조회수 높은 순": display_df = display_df.sort_values(by="Views_numeric", ascending=False)
        elif sort_option == "조회수 낮은 순": display_df = display_df.sort_values(by="Views_numeric", ascending=True)
        elif sort_option == "구독자 많은 순": display_df = display_df.sort_values(by="Subscribers_numeric", ascending=False)
        elif sort_option == "구독자 적은 순": display_df = display_df.sort_values(by="Subscribers_numeric", ascending=True)
        elif sort_option == "채널별 정렬": display_df = display_df.sort_values(by=['Channel', 'Views_numeric'], ascending=[True, False])
        
        display_df.insert(0, "선택", False)

        edited_df = st.data_editor(
            display_df,
            column_config={
                "선택": st.column_config.CheckboxColumn(required=True),
                "Thumbnail": st.column_config.ImageColumn("썸네일", help="동영상 썸네일"),
                "Views_numeric": None, 
                "Subscribers_numeric": None,
                "Hash": None # Hash 열 숨기기
            }, hide_index=True, key="results_editor"
        )
        
        selected_rows = edited_df[edited_df["선택"]]
        if not selected_rows.empty and st.button(f"{len(selected_rows)}개 항목 유튜브 결과에 추가", use_container_width=True):
            items_to_add = selected_rows.drop(columns=['선택'])
            updated_cart = pd.concat([st.session_state.shopping_cart, items_to_add]).drop_duplicates(subset=['Hash']).reset_index(drop=True)
            st.session_state.shopping_cart = updated_cart
            # save_app_data() # 파일 저장 로직 삭제
            st.success(f"{len(items_to_add)}개 항목을 유튜브 결과에 추가했습니다!")
            time.sleep(1); st.rerun()
    else:
        st.info("크롤링을 시작하면 결과가 여기에 표시됩니다.")

with tab2:
    st.header("📺 유튜브 결과 (현재 세션)")
    if not st.session_state.shopping_cart.empty:
        sort_option_cart = st.selectbox(
            "유튜브 결과 정렬",
            options=["기본", "채널별 정렬", "조회수 높은 순", "조회수 낮은 순", "구독자 많은 순", "구독자 적은 순"],
            key="sort_cart"
        )
        cart_df_with_selector = st.session_state.shopping_cart.copy()
        if sort_option_cart == "조회수 높은 순": cart_df_with_selector = cart_df_with_selector.sort_values(by="Views_numeric", ascending=False)
        elif sort_option_cart == "조회수 낮은 순": cart_df_with_selector = cart_df_with_selector.sort_values(by="Views_numeric", ascending=True)
        elif sort_option_cart == "구독자 많은 순": cart_df_with_selector = cart_df_with_selector.sort_values(by="Subscribers_numeric", ascending=False)
        elif sort_option_cart == "구독자 적은 순": cart_df_with_selector = cart_df_with_selector.sort_values(by="Subscribers_numeric", ascending=True)
        elif sort_option_cart == "채널별 정렬": cart_df_with_selector = cart_df_with_selector.sort_values(by=['Channel', 'Views_numeric'], ascending=[True, False])

        cart_df_with_selector.insert(0, "선택", False)
        
        st.info("이곳의 데이터는 앱을 종료해도 유지됩니다. 그룹으로 만들거나 다운로드할 수 있습니다.")

        edited_cart_df = st.data_editor(
            cart_df_with_selector,
            column_config={
                "선택": st.column_config.CheckboxColumn(required=True), "Thumbnail": st.column_config.ImageColumn("썸네일"),
                "Views_numeric": None, "Subscribers_numeric": None,
                "Hash": None # Hash 열 숨기기
            }, hide_index=True, key="cart_editor"
        )
        selected_cart_rows = edited_cart_df[edited_cart_df["선택"]]

        st.markdown("---")
        st.subheader("선택한 항목으로 작업하기")
        
        new_group_name = st.text_input("새 그룹 이름", placeholder="예: 7월 1주차 숏폼")
        if st.button("그룹 만들기", disabled=selected_cart_rows.empty or not new_group_name, use_container_width=True):
            if new_group_name in st.session_state.custom_groups:
                st.error(f"'{new_group_name}' 그룹이 이미 존재합니다.")
            else:
                st.session_state.custom_groups[new_group_name] = selected_cart_rows.drop(columns=['선택'])
                # save_app_data() # 파일 저장 로직 삭제
                st.success(f"'{new_group_name}' 그룹을 만들었습니다.")
                time.sleep(1); st.rerun()

        st.markdown("---") # Visual separator

        csv_cart = convert_df_to_csv(st.session_state.shopping_cart.drop(columns=['Views_numeric', 'Subscribers_numeric'], errors='ignore'))
        st.download_button("💾 CSV 다운로드 (전체)", csv_cart, f"youtube_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv", use_container_width=True)
        
        pdf_cart = convert_df_to_pdf(st.session_state.shopping_cart.drop(columns=['Views_numeric', 'Subscribers_numeric', 'Thumbnail', 'YouTube URL', 'Hash'], errors='ignore'))
        st.download_button("📄 PDF 다운로드 (전체)", pdf_cart, f"youtube_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf", "application/pdf", use_container_width=True)
        
        if st.button("전체 결과 비우기", use_container_width=True):
            st.session_state.shopping_cart = pd.DataFrame()
            # save_app_data() # 파일 저장 로직 삭제
            st.rerun()

    else:
        st.info("결과 테이블에서 항목을 선택하여 '유튜브 결과'에 추가하세요.")

    st.markdown("---")
    st.header("📂 저장된 그룹 (현재 세션)")
    if not st.session_state.custom_groups:
        st.info("만들어진 그룹이 없습니다.")
    else:
        for group_name in list(st.session_state.custom_groups.keys()):
            with st.expander(f"**{group_name}** ({len(st.session_state.custom_groups[group_name])}개 항목)"):
                group_df = st.session_state.custom_groups[group_name]
                st.dataframe(group_df, column_config={"Thumbnail": st.column_config.ImageColumn("썸네일"), "Views_numeric": None, "Subscribers_numeric": None, "Hash": None}, hide_index=True)
                if st.button(f"'{group_name}' 그룹 삭제", key=f"delete_{group_name}", use_container_width=True):
                    del st.session_state.custom_groups[group_name]
                    # save_app_data() # 파일 저장 로직 삭제
                    st.rerun()
