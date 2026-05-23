# mbid_utils.py
import os
import time
import shutil
import acoustid
from mutagen import File as MutagenFile
from dotenv import load_dotenv

load_dotenv()

ACOUSTID_API_KEY = os.getenv("ACOUSTID_API_KEY")
print("Acoustid API Key:", ACOUSTID_API_KEY)
if not ACOUSTID_API_KEY:
    raise RuntimeError("ACOUSTID_API_KEY not set")

if not shutil.which("fpcalc"):
    raise RuntimeError("fpcalc not found. Install Chromaprint and add it to PATH.")


def extract_mbid(file_path: str, score_threshold: float = 0.85):
    """
    Return (mbid, elapsed_seconds) for an audio file.

    1. Try MBID from tags (musicbrainz_trackid / UFID:https://musicbrainz.org/).
    2. If not present, use AcoustID fingerprinting.
    """
    start_time = time.time()
    mbid = None

    try:
        audio = MutagenFile(file_path)
    except Exception:
        audio = None

    # 1️⃣ Try existing MBID from tags
    if audio:
        # Picard's common tag: 'musicbrainz_trackid'
        if "musicbrainz_trackid" in audio:
            try:
                value = audio["musicbrainz_trackid"]
                # Value can be a list or similar
                if isinstance(value, (list, tuple)):
                    mbid = str(value[0])
                else:
                    mbid = str(value)
            except Exception:
                mbid = None

        # ID3 UFID case used by some taggers: UFID:https://musicbrainz.org/
        if not mbid:
            try:
                ufid_key = "UFID:https://musicbrainz.org/"
                if ufid_key in audio:
                    ufid = audio[ufid_key]
                    data = getattr(ufid, "data", None)
                    if data:
                        mbid = data.decode(errors="ignore")
            except Exception:
                pass

    # 2️⃣ Fingerprint only if MBID not found in tags
    if not mbid:
        try:
            for score, recording_id, title, artist in acoustid.match(ACOUSTID_API_KEY, file_path):
                if score >= score_threshold:
                    mbid = recording_id
                    break
        except Exception as e:
            print(f"Fingerprinting failed for {file_path}: {e}")

    elapsed = round(time.time() - start_time, 2)
    return mbid, elapsed
