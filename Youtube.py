import os
import re
import yt_dlp
import asyncio
import aiohttp
import aiofiles

from os import getenv
from typing import Optional, Union
from urllib.parse import urlparse, parse_qs

from pyrogram.types import Message
from pyrogram.enums import MessageEntityType
from youtubesearchpython.__future__ import VideosSearch

from .. import LOGGER
from ..utils.formatters import time_to_seconds



API_URL = getenv("API_URL", "https://pytube.fun")
API_KEY = getenv("API_KEY", "Your_Api_Key") #Get from @ApixhubBot

DOWNLOAD_DIR = "downloads"

GLOBAL_CLIENT_SESSION: Optional[aiohttp.ClientSession] = None
SESSION_LOCK = asyncio.Lock()


def extract_video_id(link: str) -> Optional[str]:
    if not link:
        return None
    link = link.strip()
    if "youtube.com" in link or "youtu.be" in link:
        try:
            parsed = urlparse(link)
            if "youtu.be" in parsed.netloc:
                video_id = parsed.path.strip("/")
            else:
                query = parse_qs(parsed.query)
                video_id = query.get("v", [None])[0]
            if video_id and len(video_id) >= 3:
                return video_id
            return None
        except Exception:
            return None
    return link if len(link) >= 3 else None


async def get_client_session() -> aiohttp.ClientSession:
    global GLOBAL_CLIENT_SESSION
    async with SESSION_LOCK:
        if GLOBAL_CLIENT_SESSION is None or GLOBAL_CLIENT_SESSION.closed:
            GLOBAL_CLIENT_SESSION = aiohttp.ClientSession()
    return GLOBAL_CLIENT_SESSION

  

async def close_client_session() -> None:
    global GLOBAL_CLIENT_SESSION

    try:
        if GLOBAL_CLIENT_SESSION and not GLOBAL_CLIENT_SESSION.closed:
            await GLOBAL_CLIENT_SESSION.close()
            LOGGER(__name__).info("Closed global aiohttp session")
    except Exception as e:
        LOGGER(__name__).error(f"Failed to close aiohttp session: {e}")
    finally:
        GLOBAL_CLIENT_SESSION = None


async def _download_file(
    session: aiohttp.ClientSession,
    url: str,
    file_path: str,
    timeout_seconds: int = 900,
) -> Optional[str]:
    temp_path = f"{file_path}.part"

    try:
        async with session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            allow_redirects=True,
        ) as response:
            if response.status != 200:
                LOGGER(__name__).error(
                    f"File download failed with status {response.status} for URL: {url}"
                )
                return None

            async with aiofiles.open(temp_path, "wb") as f:
                async for chunk in response.content.iter_chunked(16384):
                    await f.write(chunk)

        os.replace(temp_path, file_path)
        return file_path

    except Exception as e:
        if os.path.exists(temp_path):
            try:
                os.remove(temp_path)
            except Exception:
                pass

        LOGGER(__name__).error(f"File download failed: {e}")
        return None

async def download_media(link: str, is_video: bool) -> Union[str, None]:
    video_id = extract_video_id(link)
    if not video_id:
        return None

    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    extension = "mp4" if is_video else "mp3"
    file_path = os.path.join(DOWNLOAD_DIR, f"{video_id}.{extension}")

    if os.path.exists(file_path):
        return file_path

    api_url = f"{API_URL}/song/{video_id}"
    params = {"key": API_KEY}
    if is_video:
        params["video"] = True

    poll_interval = 1.5
    total_timeout = 30
    max_attempts = int(total_timeout / poll_interval)
    max_failures = 3
    failures = 0

    for _ in range(max_attempts):
        try:
            session = await get_client_session()

            async with session.get(
                api_url,
                params=params,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as response:
                if response.status != 200:
                    failures += 1
                    LOGGER(__name__).error(
                        f"Metadata fetch failed with status {response.status} for video_id={video_id} "
                        f"(failure {failures}/{max_failures})"
                    )

                    if failures >= max_failures:
                        return None

                    await asyncio.sleep(poll_interval)
                    continue

                data = await response.json()
                failures = 0

        except Exception as e:
            failures += 1
            LOGGER(__name__).error(
                f"Metadata fetch error for video_id={video_id}: {e} "
                f"(failure {failures}/{max_failures})"
            )

            try:
                global GLOBAL_CLIENT_SESSION
                if GLOBAL_CLIENT_SESSION and GLOBAL_CLIENT_SESSION.closed:
                    GLOBAL_CLIENT_SESSION = None
            except Exception:
                pass

            if failures >= max_failures:
                return None

            await asyncio.sleep(poll_interval)
            continue

        status = str(data.get("status", "")).lower()
        stream_url = data.get("stream_url")

        if status == "done" and stream_url:
            try:
                session = await get_client_session()
                return await _download_file(
                    session=session,
                    url=stream_url,
                    file_path=file_path,
                    timeout_seconds=900,
                )
            except Exception as e:
                LOGGER(__name__).error(f"Download start failed for video_id={video_id}: {e}")
                return None

        if status in {"failed", "error"}:
            failures += 1
            LOGGER(__name__).error(
                f"API returned status='{status}' for video_id={video_id} "
                f"(failure {failures}/{max_failures})"
            )

            if failures >= max_failures:
                return None

        await asyncio.sleep(poll_interval)

    LOGGER(__name__).error(f"Download polling timed out for video_id={video_id}")
    return None


async def shell_cmd(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, errorz = await proc.communicate()
    if errorz:
        if "unavailable videos are hidden" in (errorz.decode("utf-8")).lower():
            return out.decode("utf-8")
        else:
            return errorz.decode("utf-8")
    return out.decode("utf-8")


class YouTubeAPI:
    def __init__(self):
        self.base = "https://www.youtube.com/watch?v="
        self.regex = r"(?:youtube\.com|youtu\.be)"
        self.status = "https://www.youtube.com/oembed?url="
        self.listbase = "https://youtube.com/playlist?list="
        self.reg = re.compile(r"\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])")

    async def exists(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        return bool(re.search(self.regex, link))

    async def url(self, message_1: Message) -> Union[str, None]:
        messages = [message_1]
        if message_1.reply_to_message:
            messages.append(message_1.reply_to_message)

        for message in messages:
            if message.entities:
                for entity in message.entities:
                    if entity.type == MessageEntityType.URL:
                        text = message.text or message.caption
                        return text[entity.offset : entity.offset + entity.length]
            elif message.caption_entities:
                for entity in message.caption_entities:
                    if entity.type == MessageEntityType.TEXT_LINK:
                        return entity.url
        return None

    async def details(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
            duration_min = result.get("duration")
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
            vidid = result["id"]
            duration_sec = int(time_to_seconds(duration_min)) if duration_min else 0
        return title, duration_min, duration_sec, thumbnail, vidid

    async def title(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result["title"]

    async def duration(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result.get("duration")

    async def thumbnail(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            return result["thumbnails"][0]["url"].split("?")[0]

    async def video(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        try:
            downloaded_file = await download_media(link, is_video=True)
            if downloaded_file:
                return 1, downloaded_file
            else:
                return 0, "Video download failed"
        except Exception as e:
            return 0, f"Video download error: {e}"

    async def playlist(self, link, limit, user_id, videoid: Union[bool, str] = None):
        if videoid:
            link = self.listbase + link
        if "&" in link:
            link = link.split("&")[0]
        playlist = await shell_cmd(
            f"yt-dlp -i --get-id --flat-playlist --playlist-end {limit} --skip-download {link}"
        )
        try:
            result = [key for key in playlist.split("\n") if key]
        except Exception:
            result = []
        return result

    async def track(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        results = VideosSearch(link, limit=1)
        for result in (await results.next())["result"]:
            title = result["title"]
            duration_min = result.get("duration")
            vidid = result["id"]
            yturl = result["link"]
            thumbnail = result["thumbnails"][0]["url"].split("?")[0]
        track_details = {
            "title": title,
            "link": yturl,
            "vidid": vidid,
            "duration_min": duration_min,
            "thumb": thumbnail,
        }
        return track_details, vidid

    async def formats(self, link: str, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        ytdl_opts = {"quiet": True}
        ydl = yt_dlp.YoutubeDL(ytdl_opts)
        with ydl:
            formats_available = []
            r = ydl.extract_info(link, download=False)
            for fmt in r.get("formats", []):
                try:
                    if "dash" not in str(fmt.get("format", "")).lower():
                        formats_available.append(
                            {
                                "format": fmt.get("format"),
                                "filesize": fmt.get("filesize"),
                                "format_id": fmt.get("format_id"),
                                "ext": fmt.get("ext"),
                                "format_note": fmt.get("format_note"),
                                "yturl": link,
                            }
                        )
                except Exception:
                    continue
        return formats_available, link

    async def slider(self, link: str, query_type: int, videoid: Union[bool, str] = None):
        if videoid:
            link = self.base + link
        if "&" in link:
            link = link.split("&")[0]
        a = VideosSearch(link, limit=10)
        result = (await a.next()).get("result")
        title = result[query_type]["title"]
        duration_min = result[query_type].get("duration")
        vidid = result[query_type]["id"]
        thumbnail = result[query_type]["thumbnails"][0]["url"].split("?")[0]
        return title, duration_min, thumbnail, vidid

    async def download(
        self,
        link: str,
        mystic,
        video: Union[bool, str] = None,
        videoid: Union[bool, str] = None,
        songaudio: Union[bool, str] = None,
        songvideo: Union[bool, str] = None,
        format_id: Union[bool, str] = None,
        title: Union[bool, str] = None,
    ):
        if videoid:
            link = self.base + link

        try:
            if video:
                downloaded_file = await download_media(link, is_video=True)
            else:
                downloaded_file = await download_media(link, is_video=False)

            if downloaded_file:
                return downloaded_file, True
            return None, False
        except Exception:
            return None, False
