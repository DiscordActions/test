import os
import re
import logging
import sqlite3
import sys
from typing import List, Dict, Any, Tuple, Set
from datetime import datetime, timezone, timedelta
import time

import requests
import isodate
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# DB 설정
DB_PATH = 'youtube_videos.db'

# 환경 변수
YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
YOUTUBE_MODE = os.getenv('YOUTUBE_MODE', 'channels').lower()
YOUTUBE_CHANNEL_ID = os.getenv('YOUTUBE_CHANNEL_ID')
YOUTUBE_PLAYLIST_ID = os.getenv('YOUTUBE_PLAYLIST_ID')
YOUTUBE_PLAYLIST_SORT = os.getenv('YOUTUBE_PLAYLIST_SORT', 'default').lower()
YOUTUBE_SEARCH_KEYWORD = os.getenv('YOUTUBE_SEARCH_KEYWORD')
INIT_MAX_RESULTS = int(os.getenv('YOUTUBE_INIT_MAX_RESULTS') or '50')
MAX_RESULTS = int(os.getenv('YOUTUBE_MAX_RESULTS') or '10')
INITIALIZE_MODE_YOUTUBE = os.getenv('INITIALIZE_MODE_YOUTUBE', 'false').lower() == 'true'
ADVANCED_FILTER_YOUTUBE = os.getenv('ADVANCED_FILTER_YOUTUBE', '')
DATE_FILTER_YOUTUBE = os.getenv('DATE_FILTER_YOUTUBE', '')
DISCORD_WEBHOOK_YOUTUBE = os.getenv('DISCORD_WEBHOOK_YOUTUBE')
DISCORD_WEBHOOK_YOUTUBE_DETAILVIEW = os.getenv('DISCORD_WEBHOOK_YOUTUBE_DETAILVIEW')
DISCORD_AVATAR_YOUTUBE = os.getenv('DISCORD_AVATAR_YOUTUBE', '').strip()
DISCORD_USERNAME_YOUTUBE = os.getenv('DISCORD_USERNAME_YOUTUBE', '').strip()
LANGUAGE_YOUTUBE = os.getenv('LANGUAGE_YOUTUBE', 'English')
YOUTUBE_DETAILVIEW = os.getenv('YOUTUBE_DETAILVIEW', 'false').lower() == 'true'

# 전역 변수: 디스코드 메시지 전송을 위한 변수
discord_message_count = 0
discord_message_reset_time = time.time()
category_cache = {}

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 사용자 정의 예외
class YouTubeAPIError(Exception):
    pass

class DatabaseError(Exception):
    pass

class DiscordWebhookError(Exception):
    pass

# 유틸리티 함수
def check_env_variables() -> None:
    """환경 변수가 올바르게 설정되어 있는지 확인합니다."""
    try:
        required_vars = ['YOUTUBE_API_KEY', 'YOUTUBE_MODE', 'DISCORD_WEBHOOK_YOUTUBE']
        
        for var in required_vars:
            if not os.getenv(var):
                raise ValueError(f"필수 환경 변수 '{var}'가 설정되지 않았습니다.")

        mode = os.getenv('YOUTUBE_MODE', '').lower()
        if mode not in ['channels', 'playlists', 'search']:
            raise ValueError("YOUTUBE_MODE는 'channels', 'playlists', 'search' 중 하나여야 합니다.")

        if mode == 'channels' and not os.getenv('YOUTUBE_CHANNEL_ID'):
            raise ValueError("YOUTUBE_MODE가 'channels'일 때 YOUTUBE_CHANNEL_ID가 필요합니다.")
        elif mode == 'playlists' and not os.getenv('YOUTUBE_PLAYLIST_ID'):
            raise ValueError("YOUTUBE_MODE가 'playlists'일 때 YOUTUBE_PLAYLIST_ID가 필요합니다.")
        elif mode == 'search' and not os.getenv('YOUTUBE_SEARCH_KEYWORD'):
            raise ValueError("YOUTUBE_MODE가 'search'일 때 YOUTUBE_SEARCH_KEYWORD가 필요합니다.")

        playlist_sort = os.getenv('YOUTUBE_PLAYLIST_SORT', 'default').lower()
        if playlist_sort not in ['default', 'reverse', 'date_newest', 'date_oldest', 'position']:
            raise ValueError("YOUTUBE_PLAYLIST_SORT는 'default', 'reverse', 'date_newest', 'date_oldest', 'position' 중 하나여야 합니다.")

        for var in ['YOUTUBE_INIT_MAX_RESULTS', 'YOUTUBE_MAX_RESULTS']:
            value = os.getenv(var)
            if value and not value.isdigit():
                raise ValueError(f"{var}는 숫자여야 합니다.")

        for var in ['INITIALIZE_MODE_YOUTUBE', 'YOUTUBE_DETAILVIEW']:
            value = os.getenv(var, '').lower()
            if value and value not in ['true', 'false']:
                raise ValueError(f"{var}는 'true' 또는 'false'여야 합니다.")

        language = os.getenv('LANGUAGE_YOUTUBE', 'English')
        if language not in ['English', 'Korean']:
            raise ValueError("LANGUAGE_YOUTUBE는 'English' 또는 'Korean'이어야 합니다.")

        logging.info("환경 변수 검증 완료")
        
        safe_vars = ['YOUTUBE_MODE', 'YOUTUBE_PLAYLIST_SORT', 'YOUTUBE_INIT_MAX_RESULTS', 'YOUTUBE_MAX_RESULTS', 
                     'INITIALIZE_MODE_YOUTUBE', 'LANGUAGE_YOUTUBE', 'YOUTUBE_DETAILVIEW']
        for var in safe_vars:
            logging.info(f"{var}: {os.getenv(var)}")

    except ValueError as e:
        logging.error(f"환경 변수 검증 중 오류 발생: {e}")
        raise
    except Exception as e:
        logging.error(f"예상치 못한 오류 발생: {e}")
        raise

def parse_duration(duration: str) -> str:
    """영상 길이를 파싱합니다."""
    parsed_duration = isodate.parse_duration(duration)
    total_seconds = int(parsed_duration.total_seconds())
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    if LANGUAGE_YOUTUBE == 'Korean':
        if hours > 0:
            return f"{hours}시간 {minutes}분 {seconds}초"
        elif minutes > 0:
            return f"{minutes}분 {seconds}초"
        else:
            return f"{seconds}초"
    else:
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

def convert_to_local_time(published_at: str) -> str:
    utc_time = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ")
    utc_time = utc_time.replace(tzinfo=timezone.utc)
    
    if LANGUAGE_YOUTUBE == 'Korean':
        kst_time = utc_time + timedelta(hours=9)
        return kst_time.strftime("%Y년 %m월 %d일 %H시 %M분")
    else:
        local_time = utc_time.astimezone()
        return local_time.strftime("%Y-%m-%d %H:%M:%S") 

def apply_advanced_filter(title: str, advanced_filter: str) -> bool:
    """고급 필터를 적용하여 제목을 필터링합니다."""
    if not advanced_filter:
        return True

    text_to_check = title.lower()
    terms = re.findall(r'([+-]?)(?:"([^"]*)"|\S+)', advanced_filter)

    for prefix, term in terms:
        term = term.lower() if term else prefix.lower()
        if prefix == '+' or not prefix:  # 포함해야 하는 단어
            if term not in text_to_check:
                return False
        elif prefix == '-':  # 제외해야 하는 단어 또는 구문
            exclude_terms = term.split()
            if len(exclude_terms) > 1:
                if ' '.join(exclude_terms) in text_to_check:
                    return False
            else:
                if term in text_to_check:
                    return False

    return True

def parse_date_filter(filter_string: str) -> Tuple[datetime, datetime, datetime]:
    """날짜 필터를 파싱합니다."""
    since_date = until_date = past_date = None

    logging.info(f"파싱 중인 날짜 필터 문자열: {filter_string}")

    if not filter_string:
        logging.warning("날짜 필터 문자열이 비어있습니다.")
        return since_date, until_date, past_date

    since_match = re.search(r'since:(\d{4}-\d{2}-\d{2})', filter_string)
    until_match = re.search(r'until:(\d{4}-\d{2}-\d{2})', filter_string)
    
    if since_match:
        since_date = datetime.strptime(since_match.group(1), '%Y-%m-%d').replace(tzinfo=timezone.utc)
        logging.info(f"since_date 파싱 결과: {since_date}")
    if until_match:
        until_date = datetime.strptime(until_match.group(1), '%Y-%m-%d').replace(tzinfo=timezone.utc)
        logging.info(f"until_date 파싱 결과: {until_date}")

    past_match = re.search(r'past:(\d+)([hdmy])', filter_string)
    if past_match:
        value = int(past_match.group(1))
        unit = past_match.group(2)
        now = datetime.now(timezone.utc)
        if unit == 'h':
            past_date = now - timedelta(hours=value)
        elif unit == 'd':
            past_date = now - timedelta(days=value)
        elif unit == 'm':
            past_date = now - timedelta(days=value*30)  # 근사값 사용
        elif unit == 'y':
            past_date = now - timedelta(days=value*365)  # 근사값 사용
        logging.info(f"past_date 파싱 결과: {past_date}")
    else:
        logging.warning("past: 형식의 날짜 필터를 찾을 수 없습니다.")

    logging.info(f"최종 파싱 결과 - since_date: {since_date}, until_date: {until_date}, past_date: {past_date}")
    return since_date, until_date, past_date

def is_within_date_range(published_at: str, since_date: datetime, until_date: datetime, past_date: datetime) -> bool:
    """게시물이 날짜 필터 범위 내에 있는지 확인합니다."""
    pub_datetime = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    
    if past_date and pub_datetime >= past_date:
        return True
    if since_date and pub_datetime >= since_date:
        return True
    if until_date and pub_datetime <= until_date:
        return True
    
    return False

def get_category_name(youtube, category_id: str) -> str:
    """카테고리 ID를 카테고리 이름으로 변환합니다."""
    if category_id in category_cache:
        return category_cache[category_id]
    
    try:
        categories = youtube.videoCategories().list(part="snippet", regionCode="US").execute()
        for category in categories['items']:
            category_cache[category['id']] = category['snippet']['title']
            if category['id'] == category_id:
                return category['snippet']['title']
        return "Unknown"
    except Exception as e:
        logging.error(f"카테고리 이름을 가져오는 데 실패했습니다: {e}")
        return "Unknown"

# 데이터베이스 함수
def init_db(reset: bool = False) -> None:
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            if reset:
                c.execute("DROP TABLE IF EXISTS videos")
                logging.info("기존 videos 테이블 삭제됨")
            
            c.execute('''CREATE TABLE IF NOT EXISTS videos
                         (video_id TEXT PRIMARY KEY,
                          channel_id TEXT,
                          channel_title TEXT,
                          title TEXT,
                          description TEXT,
                          published_at TEXT,
                          thumbnail_url TEXT,
                          category_id TEXT,
                          category_name TEXT,
                          duration TEXT,
                          tags TEXT,
                          live_broadcast_content TEXT,
                          scheduled_start_time TEXT,
                          caption TEXT,
                          view_count INTEGER,
                          like_count INTEGER,
                          comment_count INTEGER,
                          source TEXT)''')
            
            conn.commit()
            
            c.execute("PRAGMA integrity_check")
            integrity_result = c.fetchone()[0]
            if integrity_result != "ok":
                logging.error(f"데이터베이스 무결성 검사 실패: {integrity_result}")
                raise sqlite3.IntegrityError("데이터베이스 무결성 검사 실패")
            
            c.execute("SELECT COUNT(*) FROM videos")
            count = c.fetchone()[0]
            
            if reset or count == 0:
                logging.info("새로운 데이터베이스가 초기화되었습니다.")
            else:
				logging.info(f"기존 데이터베이스를 사용합니다. 현재 {count}개의 항목이 있습니다.")
    except sqlite3.Error as e:
        logging.error(f"데이터베이스 초기화 중 오류 발생: {e}")
        raise DatabaseError("데이터베이스 초기화 실패")

def initialize_database_if_needed():
    try:
        if INITIALIZE_MODE_YOUTUBE:
            init_db(reset=True)
            logging.info("초기화 모드로 실행 중: 데이터베이스를 재설정하고 모든 비디오를 다시 가져옵니다.")
        else:
            init_db()
    except DatabaseError as e:
        logging.error(f"데이터베이스 초기화 중 오류 발생: {e}")
        raise

def get_existing_video_ids() -> Set[str]:
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT video_id FROM videos")
        return set(row[0] for row in c.fetchall())

def save_video(video: Dict[str, Any]):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            c = conn.cursor()
            c.execute('''
                INSERT OR REPLACE INTO videos 
                (video_id, channel_id, channel_title, title, description, published_at, 
                 thumbnail_url, category_id, category_name, duration, tags, 
                 live_broadcast_content, scheduled_start_time, caption, 
                 view_count, like_count, comment_count, source)
                VALUES 
                (:video_id, :channel_id, :channel_title, :title, :description, :published_at,
                 :thumbnail_url, :category_id, :category_name, :duration, :tags,
                 :live_broadcast_content, :scheduled_start_time, :caption,
                 :view_count, :like_count, :comment_count, :source)
            ''', video)
        logging.info(f"비디오 저장됨: {video['video_id']}")
    except sqlite3.Error as e:
        logging.error(f"데이터베이스 저장 중 오류 발생: {e}")
        raise DatabaseError("비디오 정보 저장 실패")

# YouTube API 함수
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5), retry=retry_if_exception_type(HttpError))
def build_youtube_client():
    """YouTube API 클라이언트를 생성합니다."""
    try:
        return build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    except HttpError as e:
        logging.error(f"YouTube 클라이언트 생성 중 오류 발생: {e}")
        raise YouTubeAPIError("YouTube API 클라이언트 생성 중 오류 발생")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5), retry=retry_if_exception_type(HttpError))
def fetch_videos(youtube, mode: str, channel_id: str, playlist_id: str, search_keyword: str) -> Tuple[List[Tuple[str, Dict[str, Any]]], Dict[str, str]]:
    if mode == 'channels':
        videos = fetch_channel_videos(youtube, channel_id)
        channel_info = get_channel_info(youtube, channel_id)
        return videos, channel_info
    elif mode == 'playlists':
        return fetch_playlist_videos(youtube, playlist_id)
    elif mode == 'search':
        videos = fetch_search_videos(youtube, search_keyword)
        search_info = {'title': f'Search: {search_keyword}', 'type': 'search'}
        return videos, search_info
    else:
        raise ValueError("잘못된 모드입니다.")

def get_channel_info(youtube, channel_id: str) -> Dict[str, str]:
    try:
        response = youtube.channels().list(
            part="snippet",
            id=channel_id
        ).execute()
        if 'items' in response and response['items']:
            channel_info = response['items'][0]['snippet']
            return {
                'title': channel_info['title'],
                'type': 'channel'
            }
    except Exception as e:
        logging.error(f"채널 정보를 가져오는 데 실패했습니다: {e}")
    return {'title': 'Unknown Channel', 'type': 'channel'}

def fetch_channel_videos(youtube, channel_id: str) -> List[Tuple[str, Dict[str, Any]]]:
    uploads_playlist_id = f"UU{channel_id[2:]}"
    video_items = []
    next_page_token = None
    max_results = INIT_MAX_RESULTS if INITIALIZE_MODE_YOUTUBE else MAX_RESULTS
    api_calls = 0
    max_api_calls = 3  # 최대 API 호출 횟수 제한

    logging.info(f"채널 ID: {channel_id}에서 최대 {max_results}개의 비디오를 가져오기 시작")

    while len(video_items) < max_results and api_calls < max_api_calls:
        try:
            response = youtube.playlistItems().list(
                part="snippet,contentDetails,status",
                playlistId=uploads_playlist_id,
                maxResults=min(50, max_results - len(video_items)),
                pageToken=next_page_token,
                fields="items(snippet(channelId,channelTitle,title,description,thumbnails),contentDetails(videoId,videoPublishedAt),status),nextPageToken"
            ).execute()

            for item in response.get('items', []):
                video_id = item['contentDetails']['videoId']
                snippet = item['snippet']
                published_at = item['contentDetails']['videoPublishedAt']
                status = item['status']['privacyStatus']

                # 비공개 동영상 건너뛰기
                if status == 'private':
                    continue

                video_items.append((video_id, {
                    'channelId': snippet['channelId'],
                    'channelTitle': snippet['channelTitle'],
                    'title': snippet['title'],
                    'description': snippet['description'],
                    'thumbnails': snippet['thumbnails'],
                    'publishedAt': published_at
                }))

                if len(video_items) >= max_results:
                    break

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

            api_calls += 1

        except HttpError as e:
            logging.error(f"채널 비디오 정보를 가져오는 중 오류 발생: {e}")
            raise YouTubeAPIError("채널 비디오 정보 가져오기 실패")

    logging.info(f"총 {len(video_items)}개의 비디오를 가져왔습니다.")
    
    return video_items

def fetch_playlist_videos(youtube, playlist_id: str) -> Tuple[List[Tuple[str, Dict[str, Any]]], Dict[str, str]]:
    playlist_items = []
    next_page_token = None
    max_results = INIT_MAX_RESULTS if INITIALIZE_MODE_YOUTUBE else MAX_RESULTS
    results_per_page = 50
    playlist_info = None

    try:
        playlist_response = youtube.playlists().list(
            part="snippet",
            id=playlist_id
        ).execute()
        
        if 'items' in playlist_response and playlist_response['items']:
            playlist_snippet = playlist_response['items'][0]['snippet']
            playlist_info = {
                'title': playlist_snippet['title'],
                'channel_title': playlist_snippet['channelTitle']
            }

        while len(playlist_items) < max_results:
            playlist_request = youtube.playlistItems().list(
                part="snippet,contentDetails,status",
                playlistId=playlist_id,
                maxResults=results_per_page,
                pageToken=next_page_token
            )
            playlist_response = playlist_request.execute()
            
            for item in playlist_response['items']:
                if item['status']['privacyStatus'] != 'private':
                    video_id = item['contentDetails']['videoId']
                    snippet = item['snippet']
                    playlist_items.append((video_id, snippet))
            
            next_page_token = playlist_response.get('nextPageToken')
            if not next_page_token or len(playlist_items) >= max_results:
                break

        playlist_items = sort_playlist_items(playlist_items[:max_results])
        
        return playlist_items, playlist_info

    except HttpError as e:
        logging.error(f"재생목록 정보를 가져오는 중 오류 발생: {e}")
        raise YouTubeAPIError("재생목록 비디오 정보 가져오기 실패")

def fetch_search_videos(youtube, search_keyword: str) -> List[Tuple[str, Dict[str, Any]]]:
    video_items = []
    next_page_token = None
    max_results = INIT_MAX_RESULTS if INITIALIZE_MODE_YOUTUBE else MAX_RESULTS
    api_calls = 0
    max_api_calls = 5  # API 호출 횟수 제한

    logging.info(f"검색 키워드: {search_keyword}로 최대 {max_results}개의 비디오를 가져오기 시작")

    while len(video_items) < max_results and api_calls < max_api_calls:
        try:
            response = youtube.search().list(
                q=search_keyword,
                type='video',
                part='snippet,id',
                maxResults=min(50, max_results - len(video_items)),
                pageToken=next_page_token,
                order='date'
            ).execute()

            for item in response.get('items', []):
                video_id = item['id']['videoId']
                snippet = item['snippet']
                video_items.append((video_id, snippet))

                if len(video_items) >= max_results:
                    break

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

            api_calls += 1

        except HttpError as e:
            logging.error(f"검색 결과를 가져오는 중 오류 발생: {e}")
            if e.resp.status == 403 and 'quotaExceeded' in str(e):
                logging.error("YouTube API 할당량 초과. 잠시 후 다시 시도하세요.")
            raise YouTubeAPIError("검색 비디오 정보 가져오기 실패")

    logging.info(f"총 {len(video_items)}개의 검색 결과를 가져왔습니다. API 호출 횟수: {api_calls}")
    return video_items

def sort_playlist_items(playlist_items: List[Tuple[str, Dict[str, Any]]]) -> List[Tuple[str, Dict[str, Any]]]:
    def get_published_at(item):
        return item[1].get('publishedAt') or item[1]['snippet'].get('publishedAt') or ''

    if YOUTUBE_PLAYLIST_SORT == 'reverse':
        return list(reversed(playlist_items))
    elif YOUTUBE_PLAYLIST_SORT == 'date_newest':
        return sorted(playlist_items, key=get_published_at, reverse=True)
    elif YOUTUBE_PLAYLIST_SORT == 'date_oldest':
        return sorted(playlist_items, key=get_published_at)
    elif YOUTUBE_PLAYLIST_SORT == 'position':
        return sorted(playlist_items, key=lambda x: int(x[1]['snippet'].get('position', 0)))
    else:
        return playlist_items  # default order

    logging.info(f"재생목록 정렬 완료: {YOUTUBE_PLAYLIST_SORT} 모드, {len(playlist_items)}개 항목")
    return playlist_items

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5), retry=retry_if_exception_type(HttpError))
def get_full_video_data(youtube, video_id: str, basic_info: Dict[str, Any]) -> Dict[str, Any]:
    try:
        video_response = youtube.videos().list(
            part="snippet,contentDetails,statistics,liveStreamingDetails",
            id=video_id
        ).execute()
        
        if not video_response.get('items'):
            logging.warning(f"비디오 정보를 찾을 수 없음: {video_id}")
            return None
        
        video_info = video_response['items'][0]
        content_details = video_info.get('contentDetails', {})
        statistics = video_info.get('statistics', {})
        live_streaming_details = video_info.get('liveStreamingDetails', {})
        
        return {
            'video_id': video_id,
            'video_url': f"https://youtu.be/{video_id}",
            'channel_id': basic_info['channelId'],
            'channel_title': basic_info['channelTitle'],
            'title': basic_info['title'],
            'description': basic_info['description'],
            'published_at': basic_info['publishedAt'],
            'thumbnail_url': basic_info['thumbnails']['high']['url'],
            'category_id': video_info['snippet'].get('categoryId', 'Unknown'),
            'category_name': get_category_name(youtube, video_info['snippet'].get('categoryId', 'Unknown')),
            'duration': parse_duration(content_details.get('duration', 'PT0S')),
            'tags': ','.join(video_info['snippet'].get('tags', [])),
            'live_broadcast_content': video_info['snippet'].get('liveBroadcastContent', ''),
            'scheduled_start_time': live_streaming_details.get('scheduledStartTime', ''),
            'caption': content_details.get('caption', 'false'),
            'view_count': int(statistics.get('viewCount', 0)),
            'like_count': int(statistics.get('likeCount', 0)),
            'comment_count': int(statistics.get('commentCount', 0)),
            'source': YOUTUBE_MODE
        }
    except HttpError as e:
        if e.resp.status == 403 and 'quotaExceeded' in str(e):
            logging.error("YouTube API 할당량 초과. 잠시 후 다시 시도하세요.")
            raise YouTubeAPIError("YouTube API 할당량 초과")
        logging.error(f"비디오 세부 정보를 가져오는 중 오류 발생: {e}")
        raise YouTubeAPIError("비디오 세부 정보 가져오기 실패")

def fetch_video_details(youtube, video_ids: List[str]) -> List[Dict[str, Any]]:
    """비디오 세부 정보를 가져옵니다."""
    video_details = []
    chunk_size = 50
    for i in range(0, len(video_ids), chunk_size):
        chunk = video_ids[i:i+chunk_size]
        try:
            video_details_response = youtube.videos().list(
                part="snippet,contentDetails,statistics,liveStreamingDetails",
                id=','.join(chunk)
            ).execute()
            video_details.extend(video_details_response.get('items', []))
        except Exception as e:
            logging.error(f"비디오 세부 정보를 가져오는 중 오류 발생: {e}")
    return video_details

def process_new_videos(youtube, videos: List[Tuple[str, Dict[str, Any]]], video_details_dict: Dict[str, Dict[str, Any]], 
                       existing_video_ids: Set[str], since_date: datetime, until_date: datetime, past_date: datetime) -> List[Dict[str, Any]]:
    new_videos = []
    filtered_by_date = 0
    filtered_by_advanced = 0
    
    logging.info(f"ADVANCED_FILTER_YOUTUBE: {ADVANCED_FILTER_YOUTUBE}")
    logging.info(f"DATE_FILTER_YOUTUBE: {DATE_FILTER_YOUTUBE}")
    logging.info(f"Date filter parsed - since: {since_date}, until: {until_date}, past: {past_date}")
    
    for video_id, snippet in videos:
        if video_id not in video_details_dict:
            logging.warning(f"비디오 세부 정보를 찾을 수 없음: {video_id}")
            continue

        video_detail = video_details_dict[video_id]
        snippet = video_detail['snippet']
        content_details = video_detail['contentDetails']
        statistics = video_detail.get('statistics', {})
        live_streaming_details = video_detail.get('liveStreamingDetails', {})

        published_at = snippet['publishedAt']
        
        if video_id in existing_video_ids:
            logging.info(f"이미 존재하는 비디오 건너뛰기: {video_id}")
            continue

        if not INITIALIZE_MODE_YOUTUBE and not is_within_date_range(published_at, since_date, until_date, past_date):
            logging.info(f"날짜 필터에 의해 건너뛰어진 비디오: {snippet['title']}")
            filtered_by_date += 1
            continue

        video_title = snippet['title']
        
        if not apply_advanced_filter(video_title, ADVANCED_FILTER_YOUTUBE):
            logging.info(f"고급 필터에 의해 건너뛰어진 비디오: {video_title}")
            filtered_by_advanced += 1
            continue

        new_video = {
            'video_id': video_id,
            'video_url': f"https://youtu.be/{video_id}",
            'channel_id': snippet['channelId'],
            'channel_title': snippet['channelTitle'],
            'title': snippet['title'],
            'description': snippet['description'],
            'published_at': published_at,
            'thumbnail_url': snippet['thumbnails']['high']['url'],
            'category_id': snippet.get('categoryId', 'Unknown'),
            'category_name': get_category_name(youtube, snippet.get('categoryId', 'Unknown')),
            'duration': parse_duration(content_details.get('duration', 'PT0S')),
            'tags': ','.join(snippet.get('tags', [])),
            'live_broadcast_content': snippet.get('liveBroadcastContent', ''),
            'scheduled_start_time': live_streaming_details.get('scheduledStartTime', ''),
            'caption': content_details.get('caption', 'false'),
            'view_count': int(statistics.get('viewCount', 0)),
            'like_count': int(statistics.get('likeCount', 0)),
            'comment_count': int(statistics.get('commentCount', 0)),
            'source': YOUTUBE_MODE
        }
        new_videos.append(new_video)
    
    logging.info(f"총 비디오 수: {len(videos)}")
    logging.info(f"날짜 필터에 의해 제외된 비디오 수: {filtered_by_date}")
    logging.info(f"고급 필터에 의해 제외된 비디오 수: {filtered_by_advanced}")
    logging.info(f"최종적으로 처리된 새 비디오 수: {len(new_videos)}")
    
    return new_videos

def get_channel_thumbnail(youtube, channel_id: str) -> str:
    """채널 썸네일을 가져옵니다."""
    try:
        response = youtube.channels().list(
            part="snippet",
            id=channel_id
        ).execute()
        return response['items'][0]['snippet']['thumbnails']['default']['url']
    except Exception as e:
        logging.error(f"채널 썸네일을 가져오는 데 실패했습니다: {e}")
        return ""

def fetch_playlist_info(youtube, playlist_id: str) -> Dict[str, str]:
    try:
        playlist_response = youtube.playlists().list(
            part="snippet",
            id=playlist_id
        ).execute()
        
        if 'items' in playlist_response and playlist_response['items']:
            playlist_info = playlist_response['items'][0]['snippet']
            return {
                'title': playlist_info['title'],
                'channel_title': playlist_info['channelTitle']
            }
    except Exception as e:
        logging.error(f"재생목록 정보를 가져오는 데 실패했습니다: {e}")
    
    return None

# Discord 관련 함수
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5), retry=retry_if_exception_type(requests.RequestException))
def send_to_discord(message: str, is_embed: bool = False, is_detail: bool = False) -> None:
    global discord_message_count, discord_message_reset_time

    current_time = time.time()
    if current_time - discord_message_reset_time >= 60:
        discord_message_count = 0
        discord_message_reset_time = current_time
    
    if discord_message_count >= 30:
        wait_time = 60 - (current_time - discord_message_reset_time)
        if wait_time > 0:
            logging.info(f"디스코드 API 제한에 도달했습니다. {wait_time:.2f}초 대기 중...")
            time.sleep(wait_time)
            discord_message_count = 0
            discord_message_reset_time = time.time()

    headers = {'Content-Type': 'application/json'}
    
    if is_embed:
        payload = message
    else:
        payload = {"content": message}
        if DISCORD_AVATAR_YOUTUBE:
            payload["avatar_url"] = DISCORD_AVATAR_YOUTUBE
        if DISCORD_USERNAME_YOUTUBE:
            payload["username"] = DISCORD_USERNAME_YOUTUBE
    
    webhook_url = DISCORD_WEBHOOK_YOUTUBE_DETAILVIEW if is_detail and DISCORD_WEBHOOK_YOUTUBE_DETAILVIEW else DISCORD_WEBHOOK_YOUTUBE
    
    try:
        response = requests.post(webhook_url, json=payload, headers=headers)
        response.raise_for_status()
        logging.info(f"Discord에 메시지 게시 완료 ({'상세' if is_detail else '기본'} 웹훅)")
        discord_message_count += 1
    except requests.RequestException as e:
        logging.error(f"Discord에 메시지를 게시하는 데 실패했습니다: {e}")
        raise DiscordWebhookError("Discord 웹훅 호출 중 오류 발생")
    
    time.sleep(2)  # 추가적인 속도 제한을 위한 대기

def create_discord_message(video: Dict[str, Any], formatted_published_at: str, video_url: str, playlist_info: Dict[str, str] = None) -> str:
    if LANGUAGE_YOUTUBE == 'Korean':
        return create_korean_message(video, formatted_published_at, video_url, playlist_info)
    else:
        return create_english_message(video, formatted_published_at, video_url, playlist_info)

def create_korean_message(video: Dict[str, Any], formatted_published_at: str, video_url: str, playlist_info: Dict[str, str] = None) -> str:
    source_text = get_source_text_korean(video, playlist_info)
    
    message = (
        f"{source_text}"
        f"**{video['title']}**\n"
        f"{video_url}\n\n"
        f"📁 카테고리: `{video['category_name']}`\n"
        f"⌛️ 영상 길이: `{video['duration']}`\n"
        f"📅 게시일: `{formatted_published_at}`\n"
        f"🖼️ [썸네일](<{video['thumbnail_url']}>)"
    )
    
    if video['scheduled_start_time']:
        formatted_start_time = convert_to_local_time(video['scheduled_start_time'])
        message += f"\n\n🔴 예정된 라이브 시작 시간: \n`{formatted_start_time}`"
    
    return message

def create_english_message(video: Dict[str, Any], formatted_published_at: str, video_url: str, playlist_info: Dict[str, str] = None) -> str:
    source_text = get_source_text_english(video, playlist_info)
    
    message = (
        f"{source_text}"
        f"**{video['title']}**\n"
        f"{video_url}\n\n"
        f"📁 Category: `{video['category_name']}`\n"
        f"⌛️ Duration: `{video['duration']}`\n"
        f"📅 Published: `{formatted_published_at}`\n"
        f"🖼️ [Thumbnail](<{video['thumbnail_url']}>)"
    )
    
    if video['scheduled_start_time']:
        formatted_start_time = convert_to_local_time(video['scheduled_start_time'])
        message += f"\n\n🔴 Scheduled Live Start Time: \n`{formatted_start_time}`"
    
    return message

def get_source_text_korean(video: Dict[str, Any], playlist_info: Dict[str, str] = None) -> str:
    if YOUTUBE_MODE == 'channels':
        return f"`{video['channel_title']} - YouTube`\n"
    elif YOUTUBE_MODE == 'playlists':
        if playlist_info:
            return f"`📃 {playlist_info['title']} - YouTube 재생목록 by {playlist_info['channel_title']}`\n\n`{video['channel_title']} - YouTube`\n"
        else:
            return f"`{video['channel_title']} - YouTube`\n"
    elif YOUTUBE_MODE == 'search':
        return f"`🔎 {YOUTUBE_SEARCH_KEYWORD} - YouTube 검색 결과`\n\n`{video['channel_title']} - YouTube`\n\n"
    else:
        logging.warning(f"알 수 없는 YOUTUBE_MODE: {YOUTUBE_MODE}")
        return f"`{video['channel_title']} - YouTube`\n"

def get_source_text_english(video: Dict[str, Any], playlist_info: Dict[str, str] = None) -> str:
    if YOUTUBE_MODE == 'channels':
        return f"`{video['channel_title']} - YouTube Channel`\n"
    elif YOUTUBE_MODE == 'playlists':
        if playlist_info:
            return f"`📃 {playlist_info['title']} - YouTube Playlist by {playlist_info['channel_title']}`\n\n`{video['channel_title']} - YouTube`\n"
        else:
            return f"`{video['channel_title']} - YouTube`\n"
    elif YOUTUBE_MODE == 'search':
        return f"`🔎 {YOUTUBE_SEARCH_KEYWORD} - YouTube Search Result`\n\n`{video['channel_title']} - YouTube`\n\n"
    else:
        logging.warning(f"Unknown YOUTUBE_MODE: {YOUTUBE_MODE}")
        return f"`{video['channel_title']} - YouTube`\n"

def create_embed_message(video: Dict[str, Any], youtube) -> Dict[str, Any]:
    """임베드 메시지를 생성합니다."""
	
    if 'video_url' not in video:
        logging.error(f"'video_url' 필드가 누락되었습니다: {video}")
        raise KeyError("'video_url' 필드가 누락되었습니다.")
	
    channel_thumbnail = get_channel_thumbnail(youtube, video['channel_id'])
    
    tags = video['tags'].split(',') if video['tags'] else []
    formatted_tags = ' '.join(f'`{tag.strip()}`' for tag in tags)
    
    play_text = "Play Video" if LANGUAGE_YOUTUBE == 'English' else "영상 재생"
    play_link = f"https://www.youtube.com/watch?v={video['video_id']}"
    embed_link = f"https://www.youtube.com/embed/{video['video_id']}"
    
    embed = {
        "title": video['title'],
        "description": video['description'][:4096],  # Discord 제한
        "url": video['video_url'],
        "color": 16711680,  # Red color
        "fields": [
            {
                "name": "🆔 Video ID" if LANGUAGE_YOUTUBE == 'English' else "🆔 영상 ID",
                "value": f"`{video['video_id']}`"
            },            
            {
                "name": "📁 Category" if LANGUAGE_YOUTUBE == 'English' else "📁 영상 분류",
				"value": video['category_name']
            },
            {
                "name": "🏷️ Tags" if LANGUAGE_YOUTUBE == 'English' else "🏷️ 영상 태그",
                "value": formatted_tags if formatted_tags else "N/A"
            },
            {
                "name": "⌛ Duration" if LANGUAGE_YOUTUBE == 'English' else "⌛ 영상 길이",
                "value": video['duration']
            },            
            {
                "name": "🔡 Subtitle" if LANGUAGE_YOUTUBE == 'English' else "🔡 영상 자막",
                "value": f"[Download](https://downsub.com/?url={video['video_url']})"
            },
            {
                "name": "▶️ " + play_text,
                "value": f"[Embed]({embed_link})"
            }
        ],
        "author": {
            "name": video['channel_title'],
            "url": f"https://www.youtube.com/channel/{video['channel_id']}",
            "icon_url": channel_thumbnail
        },
        "footer": {
            "text": "YouTube",
            "icon_url": "https://icon.dataimpact.ing/media/original/youtube/youtube_social_circle_red.png"
        },
        "timestamp": video['published_at'],
        "image": {
            "url": video['thumbnail_url']
        }
    }
    
    return {
        "content": None,
        "embeds": [embed],
        "attachments": []
    }

def fetch_video_data(youtube):
    logging.info("YouTube API를 통해 비디오 정보 가져오기 시작")
    videos, info = fetch_videos(youtube, YOUTUBE_MODE, YOUTUBE_CHANNEL_ID, YOUTUBE_PLAYLIST_ID, YOUTUBE_SEARCH_KEYWORD)
    
    logging.info(f"API를 통해 가져온 비디오 수: {len(videos)}")
    return videos, info

def process_videos(youtube, videos, info):
    existing_video_ids = get_existing_video_ids()
    since_date, until_date, past_date = parse_date_filter(DATE_FILTER_YOUTUBE)
    
    video_ids = [video[0] for video in videos]
    video_details = fetch_video_details(youtube, video_ids)
    video_details_dict = {video['id']: video for video in video_details}
    
    new_videos = process_new_videos(youtube, videos, video_details_dict, existing_video_ids, since_date, until_date, past_date)
    
    logging.info(f"처리할 새로운 비디오 수: {len(new_videos)}")
    
    for video in new_videos:
        save_video(video)
        send_discord_messages(video, youtube, info)
    
    return new_videos

def send_discord_messages(video, youtube, info):
    logging.info(f"처리 중인 비디오: {video['title']}")
    
    formatted_published_at = convert_to_local_time(video['published_at'])
    basic_message = create_discord_message(video, formatted_published_at, video['video_url'], info)
    send_to_discord(basic_message, is_embed=False, is_detail=False)
    
    if YOUTUBE_DETAILVIEW:
        detailed_message = create_embed_message(video, youtube)
        send_to_discord(detailed_message, is_embed=True, is_detail=True)

# 메인 실행 함수
def main():
    try:
        check_env_variables()
        initialize_database_if_needed()
        youtube = build_youtube_client()

        videos, playlist_info = fetch_video_data(youtube)
        new_videos = process_videos(youtube, videos, playlist_info)
        log_execution_info()
        
    except YouTubeAPIError as e:
        logging.error(f"유튜브 API 오류 발생: {e}")
    except DatabaseError as e:
        logging.error(f"데이터베이스 오류 발생: {e}")
    except DiscordWebhookError as e:
        logging.error(f"디스코드 웹훅 오류 발생: {e}")
    except Exception as e:
        logging.error(f"알 수 없는 오류 발생: {e}", exc_info=True)
        sys.exit(1)
    finally:
        logging.info("스크립트 실행 완료")

def log_execution_info():
    logging.info(f"YOUTUBE_MODE: {YOUTUBE_MODE}")
    logging.info(f"INITIALIZE_MODE_YOUTUBE: {INITIALIZE_MODE_YOUTUBE}")
    logging.info(f"YOUTUBE_DETAILVIEW: {YOUTUBE_DETAILVIEW}")
    logging.info(f"데이터베이스 파일 크기: {os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else '파일 없음'}")
    
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
        c.execute("SELECT COUNT(*) FROM videos")
        count = c.fetchone()[0]
        logging.info(f"데이터베이스의 비디오 수: {count}")

if __name__ == "__main__":
    main()
