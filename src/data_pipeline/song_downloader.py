import os
import logging
import re
import glob
from typing import Optional, Dict, Any, List
from yt_dlp import YoutubeDL
from pydantic import BaseModel

logger = logging.getLogger("SmartDownloader")
if not logger.handlers:
    logging.basicConfig(level=logging.INFO)

class DownloadOptions(BaseModel):
    urls: List[str]             
    mbid: str               
    title: str                  
    artist: str                 
    audio_format: str = "wav" 

class SmartDownloader:
    def __init__(self, download_dir: str = "temp_processing"):
        self.download_dir = os.path.abspath(download_dir)
        os.makedirs(self.download_dir, exist_ok=True)

    def _sanitize_filename(self, filename: str) -> str:
        return re.sub(r'[\\/*?:"<>|]', "", filename).strip()

    def _prioritize_urls(self, urls: List[str]) -> List[str]:
        """
        Sorts URLs to try the most likely working ones first.
        """
        youtube = []
        soundcloud = []
        others = []

        # List of domains yt-dlp CANNOT handle (DRM protected)
        drm_domains = [
            'spotify.com', 'open.spotify.com', 
            'apple.com', 'music.apple.com', 
            'tidal.com', 'deezer.com', 'amazon.com', 'pandora.com'
        ]

        for u in urls:
            u_lower = u.lower()
            
            # STRICT FILTER: Skip known DRM sites immediately
            if any(domain in u_lower for domain in drm_domains):
                continue
            
            # Filter weird google redirects for spotify
            if 'googleusercontent.com/spotify.com' in u_lower:
                continue

            if 'youtube.com' in u_lower or 'youtu.be' in u_lower:
                youtube.append(u)
            elif 'soundcloud.com' in u_lower:
                soundcloud.append(u)
            else:
                others.append(u)
        
        return youtube + soundcloud + others

    def _get_ydl_opts(self, filename_base: str, audio_format: str) -> Dict:
        """
        Configures yt-dlp for HIGHEST Audio Quality with Cookies support.
        """
        # Look for cookies.txt in the current directory
        cookie_path = os.path.abspath("cookies.txt")
        
        # Log warning if cookies are missing
        if not os.path.exists(cookie_path):
             logger.warning(f"⚠️ cookies.txt not found at {cookie_path}")

        return {
            'outtmpl': f"{filename_base}.%(ext)s",
            
            # 1. Source: Grab the absolute best audio stream available
            'format': 'bestaudio/best',
            
            'noplaylist': True,
            'quiet': True,
            'no_warnings': True,
            'retries': 3,
            
            # 2. Auth: Pass Cookies
            'cookiefile': cookie_path if os.path.exists(cookie_path) else None,

            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': audio_format, # 'wav'
                # 3. Quality: '0' tells FFmpeg to use the best possible quality 
                # (preserves original sample rate and bit depth)
                'preferredquality': '0', 
            }],
            
            # 4. Client: Reverted to your original working clients (Android/Web)
            'extractor_args': {'youtube': {'player_client': ['android', 'web']}}
        }

    def _try_download(self, url: str, ydl_opts: Dict) -> bool:
        try:
            with YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
            return True
        except Exception as e:
            return False

    def process(self, options: DownloadOptions) -> Dict[str, Any]:
        logger.info(f"Processing MBID: {options.mbid} | Candidates: {len(options.urls)}")

        file_base_path = os.path.join(self.download_dir, str(options.mbid))
        ydl_opts = self._get_ydl_opts(file_base_path, options.audio_format)
        
        # A. Clean up potential leftovers
        for f in glob.glob(f"{glob.escape(file_base_path)}.*"):
            try: os.remove(f) 
            except: pass

        # B. Prioritize Links
        sorted_urls = self._prioritize_urls(options.urls)
        
        success_url = None
        download_source = "Direct"

        # C. Attempt Direct Links
        for url in sorted_urls:
            if self._try_download(url, ydl_opts):
                success_url = url
                logger.info(f"  [+] Success via: {url}")
                break
        
        # D. Fallback: Search
        if not success_url:
            query = f"{options.title} {options.artist} audio"
            logger.info(f"  [-] All direct links failed. Searching: '{query}'")
            
            search_url = f"ytsearch1:{query}"
            download_source = f"Search: {options.title}"
            
            if self._try_download(search_url, ydl_opts):
                success_url = "Search Result"
            else:
                raise Exception("All links and fallback search failed.")

        # E. Verify File Exists
        found_files = glob.glob(f"{glob.escape(file_base_path)}.*")
        if not found_files:
            raise FileNotFoundError(f"Download reported success but file missing: {file_base_path}")
        
        final_file = max(found_files, key=os.path.getmtime)

        return {
            "file_path": final_file,
            "file_name": os.path.basename(final_file),
            "source": success_url or download_source,
            "mbid": options.mbid
        }