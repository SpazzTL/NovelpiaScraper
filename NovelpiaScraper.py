import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import time
import os
import sys
import json
import platform
import datetime  # For logging timestamps
import subprocess  # For automatic dependency installation
from io import BytesIO  # For handling image content in memory

# --- Custom Exception ---
class IPBanException(Exception):
    """Custom exception for suspected IP bans."""
    pass

# --- Automatic Dependency Installation Check ---
required_packages = {
    "requests": "requests",
    "bs4": "beautifulsoup4",  # Package name for pip is 'beautifulsoup4'
    "aiohttp": "aiohttp",
    "Pillow": "Pillow",  # Added Pillow as it's used for image conversion
    "exifread": "exifread"  # Added exifread for EXIF data parsing
}

missing_packages = []
for module_name, pip_name in required_packages.items():
    try:
        # Check specific top-level modules
        if module_name == "bs4":
            import bs4
        elif module_name == "aiohttp":
            import aiohttp
        elif module_name == "requests":
            import requests
        elif module_name == "Pillow":
            from PIL import Image
        elif module_name == "exifread":
            import exifread
        else:
            __import__(module_name)
    except ImportError:
        missing_packages.append(pip_name)

if missing_packages:
    print("Detected missing dependencies. Attempting to install them automatically...")
    print(f"Missing: {', '.join(missing_packages)}")
    try:
        # Use sys.executable to ensure pip is run for the current Python interpreter
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing_packages])
        print("Dependencies installed successfully. Please restart the script.")
        sys.exit(0)
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to install dependencies automatically: {e}", file=sys.stderr)
        print("Please ensure you have an active internet connection.", file=sys.stderr)
        print("You might need to run the script from an administrator command prompt, or manually install:", file=sys.stderr)
        print(f"  pip install {' '.join(missing_packages)}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during dependency installation: {e}", file=sys.stderr)
        sys.exit(1)

import asyncio
import aiohttp
from bs4 import BeautifulSoup
from PIL import Image
import exifread
import time

# --- Custom Logger Class ---
class Logger(object):
    """A custom logger that writes output to both stdout/stderr and a log file."""
    def __init__(self, filename="log.txt"):
        self.terminal = sys.stdout
        self.stderr_terminal = sys.stderr
        self.log_file_path = filename
        self.log = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)
        self.flush()

    def flush(self):
        self.terminal.flush()
        self.log.flush()

    def __enter__(self):
        sys.stdout = self
        sys.stderr = self
        self.log.write(f"--- Log for session started: {datetime.datetime.now()} ---\n\n")
        self.log.flush()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.log.write(f"\n--- Log for session ended: {datetime.datetime.now()} ---\n")
        self.log.close()
        sys.stdout = self.terminal
        sys.stderr = self.stderr_terminal

# --- Configuration ---
DEFAULT_END_ID = 999999
OUTPUT_FILE_TITLES = "novelpia_titles.txt"
OUTPUT_FILE_METADATA = "novelpia_metadata.jsonl"
DOWNLOAD_COVERS_FOLDER = "novelpia_covers"
FORBIDDEN_FILE = "forbidden.txt"
CONCURRENT_REQUESTS_LIMIT = 1
MAX_CONSECUTIVE_NETWORK_ERRORS_FOR_PROMPT = 100000
MAX_CONSECUTIVE_COVER_DOWNLOAD_ERRORS = 10

# --- Helper Functions ---
def format_duration(seconds):
    """Formats a duration in seconds to a human-readable string."""
    seconds = int(seconds)
    if seconds < 0: return "N/A"
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    if days > 0:
        return f"{days}d {hours:02}h {minutes:02}m {secs:02}s"
    return f"{hours:02}h {minutes:02}m {secs:02}s"

# --- Asynchronous HTTP Fetcher ---
async def fetch_page(session, novel_id_str, semaphore):
    """Fetches HTML content, with retry logic and IP ban detection."""
    url = f"https://novelpia.com/novel/{novel_id_str}"
    async with semaphore:
        try:
            # First Attempt
            async with session.get(url, timeout=15) as response:
                response.raise_for_status()
                html = await response.text()
                if html and html.strip():
                    return html

            # First attempt failed (blank page), retry after 5s
            print(f"\nWarning: Received blank page for {novel_id_str}. Possible rate limit. Retrying in 5s...", file=sys.stderr)
            await asyncio.sleep(5)

            # Second Attempt
            async with session.get(url, timeout=15) as response:
                response.raise_for_status()
                html = await response.text()
                if html and html.strip():
                    return html

            # Second attempt failed, wait 24h
            print("\n\n" + "#"*80, file=sys.stderr)
            print("!! WARNING: POSSIBLE IP BAN DETECTED !!".center(80), file=sys.stderr)
            print("Received a blank page again. Pausing for 24 hours.".center(80), file=sys.stderr)
            print(f"Pausing at {datetime.datetime.now()}. Will resume at {datetime.datetime.now() + datetime.timedelta(hours=24)}.".center(80), file=sys.stderr)
            print("#"*80 + "\n", file=sys.stderr)
            await asyncio.sleep(24 * 60 * 60)

            # Final Attempt (after 24h)
            print(f"\nResuming scrape. Retrying ID {novel_id_str} after 24-hour pause...", file=sys.stderr)
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()
                html = await response.text()
                if html and html.strip():
                    return html

            # **MODIFIED: Raise exception on persistent failure**
            print(f"Still receiving blank page for {novel_id_str} after 24-hour wait. Assuming IP Ban and stopping.", file=sys.stderr)
            raise IPBanException(f"Suspected IP Ban at novel ID {novel_id_str}")

        except aiohttp.ClientError as e:
            print(f"Network Error fetching page {url}: {e}", file=sys.stderr)
            return None
        except asyncio.TimeoutError:
            print(f"Timeout fetching page {url}", file=sys.stderr)
            return None
        except Exception as e:
            if not isinstance(e, IPBanException):
                print(f"Unexpected error fetching page {url}: {e}", file=sys.stderr)
            raise

async def download_cover(session, url, local_path, current_download_size_bytes_ref, max_storage_bytes):
    if current_download_size_bytes_ref[0] >= max_storage_bytes:
        return "SKIPPED_LIMIT"
    try:
        async with session.get(url, timeout=20) as response:
            response.raise_for_status()
            content = await response.read()
            if current_download_size_bytes_ref[0] + len(content) > max_storage_bytes:
                return "SKIPPED_LIMIT"
            try:
                img = Image.open(BytesIO(content))
                if img.mode == 'RGBA':
                    img = img.convert('RGB')
                img.save(local_path, "JPEG", quality=85)
                if local_path.lower().endswith((".jpg", ".jpeg")):
                    with open(local_path, 'rb') as f:
                        tags = exifread.process_file(f)
                    if 'Image FileTypeExtension' in tags:
                        exif_ext = "." + str(tags['Image FileTypeExtension']).lower().lstrip('.')
                        current_ext = os.path.splitext(local_path)[1].lower()
                        if exif_ext != current_ext:
                            new_local_path = os.path.splitext(local_path)[0] + exif_ext
                            try:
                                os.rename(local_path, new_local_path)
                                local_path = new_local_path
                            except OSError as e:
                                print(f"Error renaming file {local_path} to {new_local_path}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"Error processing image {url}: {e}", file=sys.stderr)
                with open(local_path, 'wb') as f:
                    f.write(content)
            file_size = os.path.getsize(local_path)
            current_download_size_bytes_ref[0] += file_size
            return local_path
    except aiohttp.ClientResponseError as e:
        print(f"HTTP Error downloading {url}: {e.status}", file=sys.stderr)
        return "DOWNLOAD_FAILED_HTTP_ERROR"
    except Exception as e:
        print(f"Error downloading cover {url}: {e}", file=sys.stderr)
        return "DOWNLOAD_FAILED_UNKNOWN"

# --- HTML Parser for Metadata ---
def parse_novel_data(html_content, novel_id_str):
    if not html_content: return None
    soup = BeautifulSoup(html_content, 'html.parser')
    alert_modal_div = soup.find('div', id='alert_modal', class_='modal')
    if alert_modal_div:
        modal_text = alert_modal_div.get_text(strip=True)
        # **MODIFIED: Exact text match for stopping**
        if "잘못된 소설 번호 입니다." in modal_text:
            return 'LATEST_NOVEL_REACHED'
        if "삭제된 소설 입니다." in modal_text or "잘못된 접근입니다." in modal_text:
            return None
    title_tag = soup.find('meta', attrs={'name': 'twitter:title'})
    title = re.search(r'노벨피아 - 웹소설로 꿈꾸는 세상! - (.+)', title_tag['content']).group(1).strip() if title_tag else None
    if not title: return None
    synopsis_tag = soup.find('meta', attrs={'name': 'twitter:description'})
    synopsis = synopsis_tag['content'].strip() if synopsis_tag else None
    author_tag = soup.find('a', class_='writer-name')
    author = author_tag.get_text(strip=True) if author_tag else None
    tags_container = soup.find('p', class_='writer-tag')
    tags = [span.get_text(strip=True) for span in tags_container.find_all('span', class_='tag') if span.get_text(strip=True) and '나만의태그' not in span.get_text(strip=True)] if tags_container else []
    is_adult = bool(soup.find('span', class_='b_19 s_inv', string='19'))
    publication_status = "완결" if soup.find('span', class_='b_comp s_inv', string='완결') else "연재중단" if soup.find('span', class_='s_inv', string='연재중단') else "연재중"
    cover_url_tag = soup.find('meta', attrs={'property': 'og:image'})
    cover_url = cover_url_tag['content'] if cover_url_tag and not ("novelpia.com/img/" in cover_url_tag['content'] and ".jpg" in cover_url_tag['content']) else None
    cover_mime_tag = soup.find('meta', attrs={'property': 'og:image:type'})
    cover_mime_type = cover_mime_tag['content'].strip() if cover_mime_tag else None
    like_count, chapter_count = None, None
    info_div = soup.find('div', class_='info-count2')
    if info_div:
        for p in info_div.find_all('p'):
            text, num_str = p.get_text(strip=True), re.search(r'(\d{1,3}(?:,\d{3})*)', p.get_text(strip=True))
            if num_str:
                num = int(num_str.group(1).replace(',', ''))
                if '선호' in text: like_count = num
                elif '회차' in text: chapter_count = num
    return {"id": novel_id_str, "title": title, "synopsis": synopsis, "author": author, "tags": tags, "is_adult": is_adult, "publication_status": publication_status, "cover_url": cover_url, "cover_mime_type": cover_mime_type, "cover_local_path": None, "like_count": like_count, "chapter_count": chapter_count}

# --- Main Scraper Logic ---
async def main(config):
    start_id, end_id, download_covers, max_storage_bytes, current_output_file = config['start_id'], config['end_id'], config['download_covers'], config['max_storage_bytes'], config['output_file']
    tasks_completed, found_count, covers_downloaded = 0, 0, 0
    download_size = [0]
    start_time = time.time()
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
    
    print(f"\n▶️ Starting scrape from ID {start_id:06d} to {end_id:06d}...")
    if download_covers: print(f"💽 Max cover storage: {max_storage_bytes / (1024**3):.2f} GB")

    indexed_ids, forbidden_ids = set(), set()
    if os.path.exists(FORBIDDEN_FILE):
        with open(FORBIDDEN_FILE, 'r', encoding='utf-8') as f: forbidden_ids = {line.strip() for line in f}
        if forbidden_ids: print(f"🚫 Loaded {len(forbidden_ids)} forbidden IDs.")

    if config['continue_scrape'] and current_output_file and os.path.exists(current_output_file):
        with open(current_output_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    if config['scrape_metadata']: indexed_ids.add(json.loads(line)['id'])
                    else: indexed_ids.add(re.search(r', (\d+)\n?$', line).group(1).zfill(6))
                except (json.JSONDecodeError, ValueError, AttributeError): pass
        if indexed_ids:
            print(f"✅ Found {len(indexed_ids)} already indexed novels to skip.")
            found_count = len(indexed_ids)

    f_output = open(current_output_file, 'a' if config['continue_scrape'] else 'w', encoding='utf-8') if current_output_file else None

    if download_covers:
        os.makedirs(DOWNLOAD_COVERS_FOLDER, exist_ok=True)
        download_size[0] = sum(os.path.getsize(os.path.join(r, fi)) for r, _, fs in os.walk(DOWNLOAD_COVERS_FOLDER) for fi in fs)

    try:
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36", "Referer": "https://novelpia.com/"}
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = [asyncio.create_task(process_novel(session, f"{i:06d}", semaphore, f_output, config, download_size, forbidden_ids)) for i in range(start_id, end_id + 1) if f"{i:06d}" not in indexed_ids and f"{i:06d}" not in forbidden_ids]
            if not tasks:
                print("\nNo new novels to process in the selected range. Exiting.")
                return

            print(f"Found {len(tasks)} new novels to process.")
            
            for task in asyncio.as_completed(tasks):
                try:
                    novel_id, result_status, cover_downloaded, data_written = await task
                except asyncio.CancelledError: continue
                # **MODIFIED: Catch IPBanException here**
                except IPBanException as e:
                    print(f"\n\n🚨 {e}", file=sys.stderr)
                    print("Terminating scrape due to suspected IP ban.", file=sys.stderr)
                    for t in tasks: t.cancel()
                    break

                tasks_completed += 1
                
                # **MODIFIED: More robust and clear progress logging**
                progress_percent = (tasks_completed / len(tasks)) * 100
                status_msg = f"Processed ID: {novel_id} -> '{result_status}'"
                progress_msg = f"Progress: {tasks_completed}/{len(tasks)} ({progress_percent:.2f}%)"
                print(f"{status_msg} | {progress_msg}")
                sys.stdout.flush()

                # **MODIFIED: Stop gracefully with the correct message**
                if result_status == 'latest_novel_reached':
                    print(f"\n\n🏁 Reached last known novel, {novel_id} - 잘못된 소설 번호 입니다.")
                    for t in tasks: t.cancel()
                    break

                if cover_downloaded: covers_downloaded += 1
                if data_written: found_count += 1
    finally:
        if f_output: f_output.close()
        print("\n\nScraping complete!")
        print("-" * 40)
        print(f"Total novel pages with data: {found_count}")
        print(f"Total covers downloaded this session: {covers_downloaded}")
        print(f"Total cover storage used: {download_size[0] / (1024*1024):.2f} MB")
        print(f"Total time taken: {format_duration(time.time() - start_time)}")
        print("-" * 40)

async def process_novel(session, novel_id_str, semaphore, file_handle, config, download_size_ref, forbidden_ids_set):
    html = await fetch_page(session, novel_id_str, semaphore)
    if html is None: return novel_id_str, 'network_error', False, False
    
    data = parse_novel_data(html, novel_id_str)
    if data == 'LATEST_NOVEL_REACHED':
        return novel_id_str, 'latest_novel_reached', False, False
    
    status, cover_dl, data_wr = 'skipped_no_data', False, False
    if not data:
        if novel_id_str not in forbidden_ids_set:
            forbidden_ids_set.add(novel_id_str)
            with open(FORBIDDEN_FILE, 'a', encoding='utf-8') as f: f.write(novel_id_str + '\n')
        return novel_id_str, 'skipped_forbidden', False, False

    status = 'found'
    if config['download_covers'] and data['cover_url']:
        if data['is_adult']: data['cover_local_path'] = "SKIPPED_ADULT"
        else:
            ext = os.path.splitext(data['cover_url'].split('?')[0])[1] or ".jpg"
            cover_path = os.path.join(DOWNLOAD_COVERS_FOLDER, f"{novel_id_str}{ext}")
            if os.path.exists(cover_path):
                data['cover_local_path'], cover_dl = cover_path, True
            else:
                dl_status = await download_cover(session, data['cover_url'], cover_path, download_size_ref, config['max_storage_bytes'])
                data['cover_local_path'] = dl_status
                if dl_status == cover_path: cover_dl = True
                elif "DOWNLOAD_FAILED" in dl_status: status = dl_status

    if file_handle and (config['scrape_metadata'] or config['scrape_titles_only']):
        if config['scrape_metadata']: file_handle.write(json.dumps(data, ensure_ascii=False) + '\n')
        else: file_handle.write(f"{data['title']}, {data['id']}\n")
        data_wr = True
    
    return novel_id_str, status, cover_dl, data_wr

def get_last_scraped_id(output_file, is_jsonl):
    if not os.path.exists(output_file): return -1
    last_id = -1
    with open(output_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                id_str = json.loads(line)['id'] if is_jsonl else re.search(r', (\d+)\n?$', line).group(1)
                if id_str: last_id = max(last_id, int(id_str))
            except (json.JSONDecodeError, ValueError, AttributeError): continue
    return last_id

def _get_id_range_from_user():
    while True:
        match = re.match(r'^\s*(\d+)\s*-\s*(\d+)\s*$', input(f"➡️ Enter novel ID range (e.g., 0-{DEFAULT_END_ID}): ").strip())
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            return min(start, end), max(start, end)
        print("Error: Invalid range format. Please use 'START-END'.")

def configure_scrape():
    config = {'output_file': None, 'start_id': 0, 'end_id': DEFAULT_END_ID, 'max_storage_bytes': 0}
    while True:
        choice = input("What do you want to do?\n  1. Scrape full metadata (JSONL)\n  2. Scrape titles only (TXT)\n  3. Download cover images only\nEnter choice (1/2/3): ").strip()
        if choice == '1': config.update({'scrape_metadata': True, 'scrape_titles_only': False, 'download_covers': False, 'output_file': OUTPUT_FILE_METADATA}); break
        elif choice == '2': config.update({'scrape_metadata': False, 'scrape_titles_only': True, 'download_covers': False, 'output_file': OUTPUT_FILE_TITLES}); break
        elif choice == '3': config.update({'scrape_metadata': True, 'scrape_titles_only': False, 'download_covers': True}); break
        else: print("Invalid choice.")
    
    if config['output_file']:
        last_id = get_last_scraped_id(config['output_file'], config['scrape_metadata'])
        if last_id != -1:
            while True:
                choice = input(f"\nPrevious session found. How to proceed?\n  1. Continue from last novel (ID {last_id + 1})\n  2. Enter a new custom ID range\nEnter choice (1/2): ").strip()
                if choice == '1': config.update({'start_id': last_id + 1, 'continue_scrape': True}); break
                elif choice == '2':
                    config['start_id'], config['end_id'] = _get_id_range_from_user()
                    while True:
                        ow_choice = input("\nOverwrite existing file or append?\n  1. Overwrite\n  2. Append (skip existing)\nEnter choice (1/2): ").strip()
                        if ow_choice == '1': config['continue_scrape'] = False; print("✅ Output file(s) will be overwritten."); break
                        elif ow_choice == '2': config['continue_scrape'] = True; print("✅ Script will append, skipping existing novels."); break
                        else: print("Invalid choice.")
                    break
                else: print("Invalid choice.")
        else: config['start_id'], config['end_id'] = _get_id_range_from_user(); config['continue_scrape'] = False
    else: config['start_id'], config['end_id'] = _get_id_range_from_user()

    if not config.get('download_covers') and input("\nDownload cover images as well? (y/n): ").lower().strip() == 'y':
        config['download_covers'] = True
    if config.get('download_covers'):
        while True:
            try: config['max_storage_bytes'] = float(input("Enter max storage for covers in GB (e.g., 5.0): ")) * 1024**3; break
            except ValueError: print("Invalid number.")
    return config

if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    log_file_path = os.path.join(script_dir, "log.txt")
    with Logger(log_file_path):
        try:
            scrape_config = configure_scrape()
            asyncio.run(main(scrape_config))
        except KeyboardInterrupt:
            print("\nScraping interrupted by user. Exiting gracefully.")
        except Exception as e:
            print(f"\nAn unexpected error occurred: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()
    input("\nScript finished. Press Enter to exit.")