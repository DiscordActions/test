import os
import re
import logging
import sqlite3
from typing import List, Dict, Any, Tuple
from googleapiclient.discovery import build
from datetime import datetime, timezone, timedelta
import isodate
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

# 사용자 정의 예외 클래스 추가
class YouTubeAPIError(Exception):
    pass

class DatabaseError(Exception):
    pass

class DiscordWebhookError(Exception):
    pass
    
# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

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
DISCORD_WEBHOOK_YOUTUBE = os.getenv('DISCORD_WEBHOOK_YOUTUBE')
DISCORD_WEBHOOK_YOUTUBE_DETAILVIEW = os.getenv('DISCORD_WEBHOOK_YOUTUBE_DETAILVIEW')
DISCORD_AVATAR_YOUTUBE = os.getenv('DISCORD_AVATAR_YOUTUBE', '')
DISCORD_USERNAME_YOUTUBE = os.getenv('DISCORD_USERNAME_YOUTUBE', '')
YOUTUBE_DETAILVIEW = os.getenv('YOUTUBE_DETAILVIEW', 'false').lower() == 'true'
ADVANCED_FILTER_YOUTUBE = os.getenv('ADVANCED_FILTER_YOUTUBE', '')
DATE_FILTER_YOUTUBE = os.getenv('DATE_FILTER_YOUTUBE', '')
LANGUAGE_YOUTUBE = os.getenv('LANGUAGE_YOUTUBE', 'English')

# DB 설정
DB_PATH = 'youtube_videos.db'

def check_env_variables():
    base_required_vars = ['YOUTUBE_API_KEY', 'YOUTUBE_MODE', 'DISCORD_WEBHOOK_YOUTUBE']
    mode_specific_required_vars = {
        'channels': ['YOUTUBE_CHANNEL_ID'],
        'playlists': ['YOUTUBE_PLAYLIST_ID', 'YOUTUBE_PLAYLIST_SORT'],
        'search': ['YOUTUBE_SEARCH_KEYWORD']
    }
    
    if YOUTUBE_MODE not in mode_specific_required_vars:
        raise ValueError("YOUTUBE_MODE는 'channels', 'playlists', 'search' 중 하나여야 합니다.")
    
    required_vars = base_required_vars + mode_specific_required_vars[YOUTUBE_MODE]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        raise ValueError(f"다음 환경 변수가 설정되지 않았습니다: {', '.join(missing_vars)}")
    
    if YOUTUBE_MODE == 'playlists':
        valid_sorts = ['default', 'reverse', 'date_newest', 'date_oldest']
        if YOUTUBE_PLAYLIST_SORT not in valid_sorts:
            raise ValueError(f"YOUTUBE_PLAYLIST_SORT는 {', '.join(valid_sorts)} 중 하나여야 합니다.")

def initialize_database_if_needed():
    with sqlite3.connect(DB_PATH) as conn:
        c = conn.cursor()
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
    logging.info("데이터베이스 초기화 완료")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
def build_youtube_client():
    try:
        return build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
    except Exception as e:
        logging.error(f"YouTube 클라이언트 생성 중 오류 발생: {e}")
        raise YouTubeAPIError("YouTube API 클라이언트 생성 실패")

def fetch_videos(youtube):
    if YOUTUBE_MODE == 'channels':
        return fetch_channel_videos(youtube, YOUTUBE_CHANNEL_ID)
    elif YOUTUBE_MODE == 'playlists':
        return fetch_playlist_videos(youtube, YOUTUBE_PLAYLIST_ID)
    elif YOUTUBE_MODE == 'search':
        return fetch_search_videos(youtube, YOUTUBE_SEARCH_KEYWORD)
    else:
        raise ValueError("잘못된 YOUTUBE_MODE입니다.")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
def fetch_channel_videos(youtube, channel_id: str) -> List[Tuple[str, Dict[str, Any]]]:
    uploads_playlist_id = f"UU{channel_id[2:]}"
    return fetch_playlist_videos(youtube, uploads_playlist_id)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
def fetch_playlist_videos(youtube, playlist_id: str) -> List[Tuple[str, Dict[str, Any]]]:
    video_items = []
    next_page_token = None
    max_results = INIT_MAX_RESULTS if INITIALIZE_MODE_YOUTUBE else MAX_RESULTS

    while len(video_items) < max_results:
        try:
            response = youtube.playlistItems().list(
                part="snippet,contentDetails,status",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in response.get('items', []):
                video_id = item['contentDetails']['videoId']
                snippet = item['snippet']
                video_items.append((video_id, snippet))

                if len(video_items) >= max_results:
                    break

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break

        except Exception as e:
            logging.error(f"비디오 정보를 가져오는 중 오류 발생: {e}")
            raise YouTubeAPIError("플레이리스트 비디오 정보 가져오기 실패")

    return sort_video_items(video_items)

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
def fetch_search_videos(youtube, search_keyword: str) -> List[Tuple[str, Dict[str, Any]]]:
    video_items = []
    next_page_token = None
    max_results = INIT_MAX_RESULTS if INITIALIZE_MODE_YOUTUBE else MAX_RESULTS

    while len(video_items) < max_results:
        try:
            response = youtube.search().list(
                q=search_keyword,
                type='video',
                part='snippet',
                maxResults=50,
                pageToken=next_page_token
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

        except Exception as e:
            logging.error(f"검색 결과를 가져오는 중 오류 발생: {e}")
            raise YouTubeAPIError("검색 비디오 정보 가져오기 실패")

    return video_items

def sort_video_items(video_items: List[Tuple[str, Dict[str, Any]]]) -> List[Tuple[str, Dict[str, Any]]]:
    if YOUTUBE_MODE == 'playlists' and YOUTUBE_PLAYLIST_SORT != 'default':
        if YOUTUBE_PLAYLIST_SORT == 'reverse':
            return list(reversed(video_items))
        elif YOUTUBE_PLAYLIST_SORT == 'date_newest':
            return sorted(video_items, key=lambda x: x[1]['publishedAt'], reverse=True)
        elif YOUTUBE_PLAYLIST_SORT == 'date_oldest':
            return sorted(video_items, key=lambda x: x[1]['publishedAt'])
    return video_items

def is_video_exists(video_id: str) -> bool:
    with sqlite3.connect(DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM videos WHERE video_id = ?", (video_id,))
        return cursor.fetchone() is not None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
def get_full_video_data(youtube, video_id: str, snippet: Dict[str, Any]) -> Dict[str, Any]:
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
        
        return create_video_data(youtube, video_id, video_info['snippet'], content_details, statistics, live_streaming_details)
    except HttpError as e:
        if e.resp.status == 403 and 'quotaExceeded' in str(e):
            logging.error("YouTube API 할당량 초과. 잠시 후 다시 시도하세요.")
            raise YouTubeAPIError("YouTube API 할당량 초과")
        logging.error(f"비디오 세부 정보를 가져오는 중 오류 발생: {e}")
        raise YouTubeAPIError("비디오 세부 정보 가져오기 실패")

def create_video_data(youtube, video_id: str, snippet: Dict[str, Any], content_details: Dict[str, Any], statistics: Dict[str, Any], live_streaming_details: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'video_id': video_id,
        'channel_id': snippet['channelId'],
        'channel_title': snippet['channelTitle'],
        'title': snippet['title'],
        'description': snippet.get('description', ''),
        'published_at': snippet['publishedAt'],
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
def get_category_name(youtube, category_id: str) -> str:
    try:
        categories = youtube.videoCategories().list(part="snippet", id=category_id).execute()
        return categories['items'][0]['snippet']['title']
    except Exception as e:
        logging.error(f"카테고리 이름을 가져오는 데 실패했습니다: {e}")
        return "Unknown"

def parse_duration(duration: str) -> str:
    try:
        duration_obj = isodate.parse_duration(duration)
        seconds = duration_obj.total_seconds()
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        seconds = int(seconds % 60)
        
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        else:
            return f"{minutes:02d}:{seconds:02d}"
    except Exception as e:
        logging.error(f"동영상 길이 파싱 중 오류 발생: {e}")
        return "00:00"

def save_video(video: Dict[str, Any]):
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute('''
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
def send_to_discord(video: Dict[str, Any], is_detail: bool = False):
    message = create_discord_message(video, is_detail)
    headers = {'Content-Type': 'application/json'}
    webhook_url = DISCORD_WEBHOOK_YOUTUBE_DETAILVIEW if is_detail and DISCORD_WEBHOOK_YOUTUBE_DETAILVIEW else DISCORD_WEBHOOK_YOUTUBE
    
    try:
        response = requests.post(webhook_url, json=message, headers=headers)
        response.raise_for_status()
        logging.info(f"Discord로 전송됨: {video['video_id']} ({'상세' if is_detail else '기본'} 메시지)")
    except requests.RequestException as e:
        logging.error(f"Discord에 메시지를 전송하는 데 실패했습니다: {e}")
        raise DiscordWebhookError("Discord 웹훅 전송 실패")

def create_discord_message(video: Dict[str, Any], is_detail: bool = False) -> Dict[str, Any]:
    if LANGUAGE_YOUTUBE == 'Korean':
        return create_korean_message(video, is_detail)
    else:
        return create_english_message(video, is_detail)

def create_korean_message(video: Dict[str, Any], is_detail: bool) -> Dict[str, Any]:
    message = {
        "content": None,
        "embeds": [
            {
                "title": video['title'],
                "description": video['description'][:200] + "..." if len(video['description']) > 200 else video['description'],
                "url": f"https://www.youtube.com/watch?v={video['video_id']}",
                "color": 16711680,  # 빨간색
                "fields": [
                    {"name": "채널", "value": video['channel_title'], "inline": True},
                    {"name": "재생 시간", "value": video['duration'], "inline": True},
                    {"name": "게시일", "value": convert_to_local_time(video['published_at']), "inline": True},
                    {"name": "조회수", "value": f"{video['view_count']:,}", "inline": True},
                    {"name": "좋아요", "value": f"{video['like_count']:,}", "inline": True},
                    {"name": "댓글", "value": f"{video['comment_count']:,}", "inline": True}
                ],
                "image": {"url": video['thumbnail_url']}
            }
        ]
    }
    
    if DISCORD_AVATAR_YOUTUBE:
        message["avatar_url"] = DISCORD_AVATAR_YOUTUBE
    if DISCORD_USERNAME_YOUTUBE:
        message["username"] = DISCORD_USERNAME_YOUTUBE
    
    if is_detail:
        message["embeds"][0]["fields"].extend([
            {"name": "카테고리", "value": video['category_name'], "inline": True},
            {"name": "태그", "value": video['tags'][:1000] if video['tags'] else "없음", "inline": False},
            {"name": "자막", "value": "있음" if video['caption'] == 'true' else "없음", "inline": True}
        ])
        if video['scheduled_start_time']:
            message["embeds"][0]["fields"].append(
                {"name": "예약된 시작 시간", "value": convert_to_local_time(video['scheduled_start_time']), "inline": True}
            )
    
    return message

def create_english_message(video: Dict[str, Any], is_detail: bool) -> Dict[str, Any]:
    message = {
        "content": None,
        "embeds": [
            {
                "title": video['title'],
                "description": video['description'][:200] + "..." if len(video['description']) > 200 else video['description'],
                "url": f"https://www.youtube.com/watch?v={video['video_id']}",
                "color": 16711680,  # Red color
                "fields": [
                    {"name": "Channel", "value": video['channel_title'], "inline": True},
                    {"name": "Duration", "value": video['duration'], "inline": True},
                    {"name": "Published", "value": convert_to_local_time(video['published_at']), "inline": True},
                    {"name": "Views", "value": f"{video['view_count']:,}", "inline": True},
                    {"name": "Likes", "value": f"{video['like_count']:,}", "inline": True},
                    {"name": "Comments", "value": f"{video['comment_count']:,}", "inline": True}
                ],
                "image": {"url": video['thumbnail_url']}
            }
        ]
    }
    
    if DISCORD_AVATAR_YOUTUBE:
        message["avatar_url"] = DISCORD_AVATAR_YOUTUBE
    if DISCORD_USERNAME_YOUTUBE:
        message["username"] = DISCORD_USERNAME_YOUTUBE
    
    if is_detail:
        message["embeds"][0]["fields"].extend([
            {"name": "Category", "value": video['category_name'], "inline": True},
            {"name": "Tags", "value": video['tags'][:1000] if video['tags'] else "None", "inline": False},
            {"name": "Caption", "value": "Available" if video['caption'] == 'true' else "Not available", "inline": True}
        ])
        if video['scheduled_start_time']:
            message["embeds"][0]["fields"].append(
                {"name": "Scheduled Start Time", "value": convert_to_local_time(video['scheduled_start_time']), "inline": True}
            )
    
    return message

def convert_to_local_time(time_string: str) -> str:
    utc_time = datetime.strptime(time_string, "%Y-%m-%dT%H:%M:%SZ")
    utc_time = utc_time.replace(tzinfo=timezone.utc)
    local_time = utc_time.astimezone()
    
    if LANGUAGE_YOUTUBE == 'Korean':
        return local_time.strftime("%Y년 %m월 %d일 %H시 %M분")
    else:
        return local_time.strftime("%Y-%m-%d %H:%M:%S")

def parse_date_filter(filter_string: str) -> Tuple[datetime, datetime, datetime]:
    since_date = None
    until_date = None
    past_date = None

    since_match = re.search(r'since:(\d{4}-\d{2}-\d{2})', filter_string)
    until_match = re.search(r'until:(\d{4}-\d{2}-\d{2})', filter_string)
    past_match = re.search(r'past:(\d+)([hdmy])', filter_string)

    if since_match:
        since_date = datetime.strptime(since_match.group(1), '%Y-%m-%d').replace(tzinfo=timezone.utc)
    if until_match:
        until_date = datetime.strptime(until_match.group(1), '%Y-%m-%d').replace(tzinfo=timezone.utc)
    if past_match:
        value = int(past_match.group(1))
        unit = past_match.group(2)
        now = datetime.now(timezone.utc)
        if unit == 'h':
            past_date = now - timedelta(hours=value)
        elif unit == 'd':
            past_date = now - timedelta(days=value)
        elif unit == 'm':
            past_date = now - timedelta(days=value*30)
        elif unit == 'y':
            past_date = now - timedelta(days=value*365)

    return since_date, until_date, past_date

def apply_advanced_filter(title: str, description: str, advanced_filter: str) -> bool:
    if not advanced_filter:
        return True

    text_to_check = (title + " " + description).lower()
    terms = re.findall(r'([+-]?)(?:"([^"]*)"|\S+)', advanced_filter)

    for prefix, term in terms:
        term = term.lower() if term else prefix.lower()
        if prefix == '+' or not prefix:  # 포함해야 하는 단어
            if term not in text_to_check:
                return False
        elif prefix == '-':  # 제외해야 하는 단어
            if term in text_to_check:
                return False

    return True

def is_within_date_range(published_at: str, since_date: datetime, until_date: datetime, past_date: datetime) -> bool:
    pub_datetime = datetime.strptime(published_at, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    
    if past_date and pub_datetime < past_date:
        return False
    if since_date and pub_datetime < since_date:
        return False
    if until_date and pub_datetime > until_date:
        return False
    
    return True

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=5))
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
        raise YouTubeAPIError("재생목록 정보 가져오기 실패")
    
    return None

def process_videos(youtube):
    try:
        videos = fetch_videos(youtube)
        new_videos = []
        playlist_info = None

        if YOUTUBE_MODE == 'playlists':
            playlist_info = fetch_playlist_info(youtube, YOUTUBE_PLAYLIST_ID)

        since_date, until_date, past_date = parse_date_filter(DATE_FILTER_YOUTUBE)

        for video_id, snippet in videos:
            if not is_video_exists(video_id):
                try:
                    video_data = get_full_video_data(youtube, video_id, snippet)
                    if video_data and is_within_date_range(video_data['published_at'], since_date, until_date, past_date) and \
                    apply_advanced_filter(video_data['title'], video_data['description'], ADVANCED_FILTER_YOUTUBE):
                        new_videos.append(video_data)
                except YouTubeAPIError as e:
                    logging.error(f"비디오 {video_id} 처리 중 오류 발생: {e}")
                    continue

        new_videos.sort(key=lambda x: x['published_at'])

        for video in new_videos:
            try:
                save_video(video)
                send_to_discord(video)
                if YOUTUBE_DETAILVIEW:
                    send_to_discord(video, is_detail=True)
            except (DatabaseError, DiscordWebhookError) as e:
                logging.error(f"비디오 {video['video_id']} 저장 또는 전송 중 오류 발생: {e}")

        logging.info(f"총 {len(new_videos)}개의 새 비디오를 처리했습니다.")
    except Exception as e:
        logging.error(f"비디오 처리 중 예기치 않은 오류 발생: {e}")

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

def main():
    try:
        check_env_variables()
        initialize_database_if_needed()
        youtube = build_youtube_client()
        process_videos(youtube)
        log_execution_info()
    except YouTubeAPIError as e:
        logging.error(f"YouTube API 오류: {e}")
    except Exception as e:
        logging.error(f"실행 중 오류 발생: {e}", exc_info=True)
    finally:
        logging.info("스크립트 실행 완료")

if __name__ == "__main__":
    main()
