import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import time
import os
import sys
import json
import platform
import datetime # For logging timestamps
import subprocess # For automatic dependency installation
from io import BytesIO # For handling image content in memory
from PIL import Image # Ensure Image is imported for cover conversion
import exifread # Ensure exifread is imported
import random # For random delays and User-Agent selection

# --- Automatic Dependency Installation Check ---
try:
    import importlib.metadata
    import subprocess
except ImportError:
    print("FATAL ERROR: Could not import core modules. Please ensure you are using a standard Python 3.8+ installation.")
    sys.exit(1)

required_packages = [
    "requests",
    "beautifulsoup4",
    "aiohttp",
    "Pillow",
    "exifread"
]

missing_packages = []
for package_name in required_packages:
    try:
        importlib.metadata.version(package_name)
    except importlib.metadata.PackageNotFoundError:
        missing_packages.append(package_name)

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
# --- End of Dependency Check ---

# --- Custom Logger Class ---
class Logger(object):
    """
    A custom logger that writes output to both stdout/stderr and a log file.
    The log file is cleared at the beginning of each script execution.
    """
    def __init__(self, filename="log.txt"):
        self.terminal = sys.stdout
        self.log_file_path = filename
        self.log = open(filename, "w", encoding="utf-8")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

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
        sys.stderr = self.terminal

# --- Configuration (Defaults, will be overridden by user input) ---
DEFAULT_START_ID = 0 # Default start ID
DEFAULT_END_ID = 999999 # Default end ID
OUTPUT_FILE_TITLES = "novelpia_titles.txt"
OUTPUT_FILE_METADATA = "novelpia_metadata.jsonl"
DOWNLOAD_COVERS_FOLDER = "novelpia_covers"
FORBIDDEN_FILE = "forbidden.txt"
CONCURRENT_REQUESTS_LIMIT = 5 # Increase default for potentially better throughput with delays

# User-Agent list for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/120.0.6099.109 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 10; SM-G981B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36"
]

# --- Custom Exception ---
class IPBanException(Exception):
    """Custom exception for suspected IP bans."""
    pass

# --- Asynchronous HTTP Fetcher ---
async def fetch_page(session, novel_id_str, semaphore, min_delay, max_delay):
    """Fetches HTML content, with retry logic and IP ban detection."""
    url = f"https://novelpia.com/novel/{novel_id_str}"
    
    # Introduce random delay
    await asyncio.sleep(random.uniform(min_delay, max_delay))

    # Select a random User-Agent for this request
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://novelpia.com/"
    }

    async with semaphore:
        try:
            # First Attempt
            async with session.get(url, headers=headers, timeout=15) as response:
                response.raise_for_status()
                html = await response.text()
                if html and html.strip():
                    return html

            # First attempt failed (blank page), retry after 5s
            print(f"\nWarning: Received blank page for {novel_id_str}. Possible rate limit. Retrying in 5s...", file=sys.stderr)
            await asyncio.sleep(5)

            # Second Attempt
            async with session.get(url, headers=headers, timeout=15) as response:
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
            async with session.get(url, headers=headers, timeout=30) as response:
                response.raise_for_status()
                html = await response.text()
                if html and html.strip():
                    return html

            # Raise exception on persistent failure
            print(f"Still receiving blank page for {novel_id_str} after 24-hour wait. Assuming IP Ban and stopping.", file=sys.stderr)
            raise IPBanException(f"Suspected IP Ban at novel ID {novel_id_str}")

        except aiohttp.ClientError as e:
            print(f"Network Error fetching page {url}: {e}", file=sys.stderr)
            return None
        except asyncio.TimeoutError:
            print(f"Timeout fetching page {url}", file=sys.stderr)
            return None
        except Exception as e:
            if not isinstance(e, IPBanException): # Ensure we don't catch our own IPBanException here
                print(f"Unexpected error fetching page {url}: {e}", file=sys.stderr)
            raise # Re-raise other exceptions, especially IPBanException

async def download_cover(session, url, local_path, current_download_size_bytes_ref, max_storage_bytes, min_delay, max_delay):
    # Introduce random delay for cover downloads too
    await asyncio.sleep(random.uniform(min_delay, max_delay))

    # Select a random User-Agent for this request
    headers = {
        "User-Agent": random.choice(USER_AGENTS),
        "Referer": "https://novelpia.com/"
    }

    if current_download_size_bytes_ref[0] >= max_storage_bytes:
        return "SKIPPED_LIMIT"
    try:
        async with session.get(url, headers=headers, timeout=20) as response:
            response.raise_for_status()
            content = await response.read()
            if current_download_size_bytes_ref[0] + len(content) > max_storage_bytes:
                return "SKIPPED_LIMIT"
            try:
                img = Image.open(BytesIO(content))
                if img.mode == 'RGBA':
                    img = img.convert('RGB')
                img.save(local_path, "JPEG", quality=85)
                # --- EXIF Data Check and File Type Correction ---
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
                # Fallback to direct write if Pillow/EXIF fails
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
    """Parses the HTML content to extract novel title, synopsis, author, tags, age rating, publication status, cover URL, like count, and chapter count.
    Returns 'LATEST_NOVEL_REACHED' if the page indicates the end of valid novel IDs.
    Returns a dictionary with 'status' indicating 'deleted_novel' or 'access_denied_novel' if those specific messages are found.
    Returns None if the page is truly unparseable (e.g., no title found).
    """
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, 'html.parser')

    # Check for "deleted novel" or "incorrect access" indicator immediately
    alert_modal_div = soup.find('div', id='alert_modal', class_='modal')
    if alert_modal_div:
        modal_text = alert_modal_div.get_text(strip=True)
        if "잘못된 소설 번호 입니다." in modal_text:
            return 'LATEST_NOVEL_REACHED'
        elif "삭제된 소설 입니다." in modal_text:
            return {"id": novel_id_str, "status": "deleted_novel", "title": "DELETED NOVEL", "synopsis": None, "author": None, "tags": [], "is_adult": False, "publication_status": "삭제됨", "cover_url": None, "cover_mime_type": None, "cover_local_path": None, "like_count": None, "chapter_count": None}
        elif "잘못된 접근입니다." in modal_text:
            return {"id": novel_id_str, "status": "access_denied_novel", "title": "ACCESS DENIED NOVEL", "synopsis": None, "author": None, "tags": [], "is_adult": False, "publication_status": "접근불가", "cover_url": None, "cover_mime_type": None, "cover_local_path": None, "like_count": None, "chapter_count": None}

    # 1. Extract Title
    title = None
    meta_title_tag = soup.find('meta', attrs={'name': 'twitter:title'})
    if meta_title_tag and 'content' in meta_title_tag.attrs:
        full_title = meta_title_tag['content']
        match = re.search(r'노벨피아 - 웹소설로 꿈꾸는 세상! - (.+)', full_title)
        if match:
            title = match.group(1).strip()

    # If no title, it's likely not a valid novel page, return None
    if not title:
        return None

    # 2. Extract Synopsis
    synopsis = None
    meta_desc_tag = soup.find('meta', attrs={'name': 'twitter:description'})
    if meta_desc_tag and 'content' in meta_desc_tag.attrs:
        synopsis = meta_desc_tag['content'].strip()

    # 3. Extract Author
    author = None
    author_tag = soup.find('a', class_='writer-name')
    if author_tag:
        author = author_tag.get_text(strip=True)

    # 4. Extract Tags
    tags = []
    tags_container = soup.find('p', class_='writer-tag')
    if tags_container:
        for tag_span in tags_container.find_all('span', class_='tag'):
            tag_text = tag_span.get_text(strip=True)
            # Exclude the "Add my own tag" button
            if tag_text and tag_text != '+나만의태그 추가':
                tags.append(tag_text)

    # 5. Extract Age Verification (Adult/성인)
    is_adult = False
    # Look for <span class="b_19 s_inv">19</span>
    age_tag = soup.find('span', class_='b_19 s_inv')
    if age_tag and age_tag.get_text(strip=True) == '19':
        is_adult = True

    # 6. Extract Publication Status
    publication_status = "연재중" # Default to "serializing"
    # Look for <span class="b_comp s_inv">완결</span>
    complete_tag = soup.find('span', class_='b_comp s_inv')
    if complete_tag and complete_tag.get_text(strip=True) == '완결':
        publication_status = "완결"
    else:
        # Look for <span class="s_inv" style="...">연재중단</span>
        discontinued_tag = soup.find('span', class_='s_inv', string='연재중단')
        if discontinued_tag:
            publication_status = "연재중단"

    # 7. Extract Cover Image URL
    cover_url = None
    cover_mime_type = None # Initialize mime type
    og_image_tag = soup.find('meta', attrs={'property': 'og:image'})
    if og_image_tag and 'content' in og_image_tag.attrs:
        extracted_url = og_image_tag['content']
        # Skip known placeholder images
        if not ("novelpia.com/img/" in extracted_url and ".jpg" in extracted_url):
            cover_url = extracted_url

    # Try to get mime type from og:image:type meta tag
    og_image_type_tag = soup.find('meta', attrs={'property': 'og:image:type'})
    if og_image_type_tag and 'content' in og_image_type_tag.attrs:
        cover_mime_type = og_image_type_tag['content'].strip()


    # --- MODIFIED: Extract Like Count and Chapter Count more robustly ---
    like_count, chapter_count = None, None
    info_div = soup.find('div', class_='info-count2')
    if info_div:
        for p in info_div.find_all('p'):
            text = p.get_text(strip=True)
            num_str_match = re.search(r'(\d{1,3}(?:,\d{3})*)', text) # Use num_str_match instead of num_str direct
            if num_str_match:
                num = int(num_str_match.group(1).replace(',', ''))
                if '선호' in text: like_count = num
                elif '회차' in text: chapter_count = num

    return {
        "id": novel_id_str,
        "title": title,
        "synopsis": synopsis,
        "author": author,
        "tags": tags,
        "is_adult": is_adult,
        "publication_status": publication_status,
        "cover_url": cover_url,
        "cover_mime_type": cover_mime_type,
        "cover_local_path": None, # Placeholder for local path, will be filled later
        "like_count": like_count,
        "chapter_count": chapter_count
    }

async def process_novel(session, novel_id_str, semaphore, file_handle,
                        scrape_metadata_flag, scrape_titles_only_flag,
                        download_covers_flag,
                        current_download_size_bytes_ref, max_storage_bytes,
                        forbidden_novel_ids_set, min_delay, max_delay,
                        scrape_skipped_novels_flag): # New argument for scraping skipped novels
    """Fetches, parses, and writes a single novel's data, and optionally downloads its cover.
    Returns a tuple: (novel_id, status_string, cover_downloaded_flag, data_written_flag)
    """
    html_content = await fetch_page(session, novel_id_str, semaphore, min_delay, max_delay)

    if html_content is None:
        return novel_id_str, 'network_error', False, False # Indicate a network-related error, no cover, no data

    data = parse_novel_data(html_content, novel_id_str)

    if data == 'LATEST_NOVEL_REACHED':
        return novel_id_str, 'latest_novel_reached', False, False

    cover_downloaded_this_novel = False
    data_written_this_novel = False
    status = 'skipped_no_data' # Default status

    if data:
        # If data is a dictionary, it's either a valid novel or a specifically identified skipped one
        if isinstance(data, dict) and data.get('status') in ["deleted_novel", "access_denied_novel"]:
            if scrape_skipped_novels_flag:
                status = data['status'] # Use the status from parse_novel_data
                # Do NOT add to forbidden_ids_set if we are scraping them
            else:
                # If not scraping skipped novels, treat as forbidden
                if novel_id_str not in forbidden_novel_ids_set:
                    forbidden_novel_ids_set.add(novel_id_str)
                    with open(FORBIDDEN_FILE, 'a', encoding='utf-8') as f_forbidden:
                        f_forbidden.write(novel_id_str + '\n')
                return novel_id_str, 'skipped_forbidden', False, False
        else:
            status = 'found' # Valid novel data

        # Handle cover download logic (only if it's a valid novel or we're scraping skipped and it has a URL)
        # For 'deleted_novel' or 'access_denied_novel', cover_url will be None, so this block won't run for them
        if download_covers_flag and data['cover_url']:
            if data['is_adult']:
                data['cover_local_path'] = "SKIPPED_ADULT"
            else:
                # Determine the correct file extension from the URL
                url_path = data['cover_url'].split('?')[0] # Remove query parameters if any
                file_extension = os.path.splitext(url_path)[1]
                if not file_extension or file_extension.lower() not in ['.png', '.jpg', '.jpeg', '.gif', '.webp']:
                    file_extension = ".jpg" # Default to JPG if unknown/invalid

                cover_filename = f"{novel_id_str}{file_extension}"
                local_cover_path = os.path.join(DOWNLOAD_COVERS_FOLDER, cover_filename)

                if os.path.exists(local_cover_path):
                    data['cover_local_path'] = local_cover_path
                    cover_downloaded_this_novel = True # Count as "available" cover
                elif current_download_size_bytes_ref[0] >= max_storage_bytes:
                    data['cover_local_path'] = "SKIPPED_LIMIT"
                else:
                    download_status = await download_cover(
                        session, data['cover_url'], local_cover_path,
                        current_download_size_bytes_ref, max_storage_bytes,
                        min_delay, max_delay
                    )
                    data['cover_local_path'] = download_status
                    if download_status == local_cover_path:
                        cover_downloaded_this_novel = True
                    elif "DOWNLOAD_FAILED" in download_status:
                        # If cover download failed, update the status to reflect this
                        # But only if it's not already a 'deleted' or 'access_denied' status
                        if status not in ["deleted_novel", "access_denied_novel"]:
                            status = download_status

        # Handle data writing logic
        if file_handle:
            if scrape_metadata_flag:
                file_handle.write(json.dumps(data, ensure_ascii=False) + '\n')
                data_written_this_novel = True
            elif scrape_titles_only_flag:
                # For titles only, we need to decide how to represent skipped novels
                if data.get('status') in ["deleted_novel", "access_denied_novel"]:
                    file_handle.write(f"ID: {data['id']}, Status: {data['status']}\n")
                else:
                    file_handle.write(f"{data['title']}, {data['id']}\n")
                data_written_this_novel = True
    else:
        # If data is None here, it means parse_novel_data returned None (truly unparseable page, not deleted/access denied)
        # These are still genuinely skipped and not added to forbidden.txt as they are not "known" novel IDs
        pass # No change in status, remains 'skipped_no_data'

    return novel_id_str, status, cover_downloaded_this_novel, data_written_this_novel

def get_last_scraped_id(output_file, is_metadata_file):
    """
    Reads the last novel ID from an existing output file to resume scraping.
    Returns the last ID found, or -1 if the file is empty or does not exist.
    """
    last_id = -1
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                for line in f:
                    try:
                        if is_metadata_file: # JSONL
                            data = json.loads(line)
                            current_id = int(data.get('id', -1))
                        else: # TXT (titles only)
                            # Handle both normal and skipped novel format in TXT
                            match = re.search(r', (\d{6})\n?$', line) # For normal titles
                            if not match:
                                match = re.search(r'ID: (\d{6})', line) # For skipped status
                            current_id = int(match.group(1)) if match else -1
                        if current_id > last_id:
                            last_id = current_id
                    except (json.JSONDecodeError, ValueError, AttributeError):
                        # Skip unparseable lines
                        continue
        except Exception as e:
            print(f"Error reading existing file {output_file}: {e}", file=sys.stderr)
            return -1 # Indicate an error in reading, so start fresh or handle manually
    return last_id

def _get_id_range_from_user():
    """
    Prompts the user for a novel ID range (e.g., "1-100").
    Handles default values and input validation.
    Returns (start_id, end_id) tuple.
    """
    while True:
        range_input = input(f"➡️ Enter novel ID range (e.g., 0-{DEFAULT_END_ID}): ").strip()
        match = re.match(r'^\s*(\d+)\s*-\s*(\d+)\s*$', range_input)
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            return min(start, end), max(start, end)
        print("Error: Invalid range format. Please use 'START-END'.")

def configure_scrape():
    config = {'output_file': None, 'start_id': 0, 'end_id': DEFAULT_END_ID, 'max_storage_bytes': 0,
              'scrape_metadata': False, 'scrape_titles_only': False, 'download_covers': False, 'continue_scrape': False,
              'min_delay': 0.5, 'max_delay': 1.5, 'ignore_forbidden_file': False, 'scrape_skipped_novels': False}

    while True:
        choice = input("What do you want to do?\n  1. Scrape full metadata (JSONL)\n  2. Scrape titles only (TXT)\n  3. Download cover images only\nEnter choice (1/2/3): ").strip()
        if choice == '1': config.update({'scrape_metadata': True, 'output_file': OUTPUT_FILE_METADATA}); break
        elif choice == '2': config.update({'scrape_titles_only': True, 'output_file': OUTPUT_FILE_TITLES}); break
        elif choice == '3': config.update({'download_covers': True}); break # scrape_metadata=True implied for covers only to allow 'is_adult' check
        else: print("Invalid choice.")

    # Determine ID range and continuation logic
    if config['output_file']: # This block applies if we are scraping metadata or titles
        last_id = get_last_scraped_id(config['output_file'], config['scrape_metadata'])
        if last_id != -1:
            while True:
                choice = input(f"\nPrevious session found. How to proceed?\n  1. Continue from last novel (ID {last_id + 1})\n  2. Enter a new custom ID range\nEnter choice (1/2): ").strip()
                if choice == '1':
                    config.update({'start_id': last_id + 1, 'continue_scrape': True})
                    print(f"✅ Will continue from ID {config['start_id']}.")
                    break
                elif choice == '2':
                    config['start_id'], config['end_id'] = _get_id_range_from_user()
                    while True:
                        ow_choice = input("\nOverwrite existing file or append (skip existing)?\n  1. Overwrite\n  2. Append\nEnter choice (1/2): ").strip()
                        if ow_choice == '1': config['continue_scrape'] = False; print("✅ Output file(s) will be overwritten."); break
                        elif ow_choice == '2': config['continue_scrape'] = True; print("✅ Script will append, skipping existing novels."); break
                        else: print("Invalid choice.")
                    break
                else: print("Invalid choice.")
        else: # No previous data found in the output file
            config['start_id'], config['end_id'] = _get_id_range_from_user()
            config['continue_scrape'] = False # Default to not continuing if no file found, acts as overwrite
    else: # This block applies if we are only downloading covers (choice 3)
        config['start_id'], config['end_id'] = _get_id_range_from_user()
        config['continue_scrape'] = False # No continuation concept for covers-only mode for main data files
        print("Note: In 'Download cover images only' mode, existing covers are skipped but no new metadata/title files are created for continuation.")

    # Always ask about downloading covers if not the primary choice
    if not config.get('download_covers') and input("\nDownload cover images as well? (y/n): ").lower().strip() == 'y':
        config['download_covers'] = True

    if config.get('download_covers'):
        os.makedirs(DOWNLOAD_COVERS_FOLDER, exist_ok=True)
        print(f"Covers will be saved to: {DOWNLOAD_COVERS_FOLDER}")
        while True:
            try:
                storage_limit_gb = float(input("Enter max storage for covers in GB (e.g., 5.0): "))
                config['max_storage_bytes'] = storage_limit_gb * 1024**3 # Convert GB to Bytes
                print(f"Maximum cover storage limit: {storage_limit_gb:.2f} GB\n")
                break
            except ValueError: print("Invalid number.")

    # Get user input for delay range
    while True:
        try:
            min_d = float(input(f"Enter minimum delay between requests in seconds (e.g., {config['min_delay']}): "))
            max_d = float(input(f"Enter maximum delay between requests in seconds (e.g., {config['max_delay']}): "))
            if min_d < 0 or max_d < 0 or min_d > max_d:
                print("Invalid delay range. Min must be non-negative and less than or equal to Max.")
            else:
                config['min_delay'] = min_d
                config['max_delay'] = max_d
                print(f"✅ Delay between requests: {min_d:.2f} - {max_d:.2f} seconds (randomized).")
                break
        except ValueError:
            print("Invalid number for delay.")
            
    # New option: Ignore forbidden.txt
    while True:
        ignore_forbidden_choice = input("Do you want to ignore the 'forbidden.txt' file and attempt to scrape those IDs? (y/n): ").lower().strip()
        if ignore_forbidden_choice == 'y':
            config['ignore_forbidden_file'] = True
            print("✅ 'forbidden.txt' will be ignored.")
            break
        elif ignore_forbidden_choice == 'n':
            config['ignore_forbidden_file'] = False
            print("✅ 'forbidden.txt' will be used to skip IDs.")
            break
        else:
            print("Invalid input. Please enter 'y' or 'n'.")

    # New option: Scrape deleted/access denied novels
    if config['scrape_metadata']: # Only relevant if scraping metadata
        while True:
            scrape_skipped_choice = input("Do you want to scrape novels that return 'deleted' or 'access denied' status? (y/n): ").lower().strip()
            if scrape_skipped_choice == 'y':
                config['scrape_skipped_novels'] = True
                print("✅ Deleted/access denied novels will be scraped and included in metadata.")
                break
            elif scrape_skipped_choice == 'n':
                config['scrape_skipped_novels'] = False
                print("✅ Deleted/access denied novels will be skipped and added to 'forbidden.txt'.")
                break
            else:
                print("Invalid input. Please enter 'y' or 'n'.")
    else:
        # If not scraping metadata, this option is less relevant or handled differently
        config['scrape_skipped_novels'] = False # Default to false if not scraping full metadata

    return config

async def main(config):
    """Main function to orchestrate the scraping process."""
    total_tasks_created = 0 # This will be the denominator for progress calculation
    tasks_completed = 0 # This will be the numerator for progress calculation
    found_count = 0 # Count of novels where data was written to file
    covers_downloaded = 0 # Count of covers actually downloaded or already existed
    current_download_size_bytes = [0] # Use a list to pass by reference for mutable update
    start_time = time.time()
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)

    print(f"Starting Novelpia scraping from ID {config['start_id']:06d} to {config['end_id']:06d}...")
    print(f"Concurrent requests limit: {CONCURRENT_REQUESTS_LIMIT}")
    if config.get('download_covers'):
        print(f"Maximum cover storage limit: {config['max_storage_bytes'] / (1024**3):.2f} GB")

    indexed_ids = set()
    f_output = None
    forbidden_ids = set()

    # Load forbidden IDs from file, unless ignoring
    if os.path.exists(FORBIDDEN_FILE) and not config['ignore_forbidden_file']:
        try:
            with open(FORBIDDEN_FILE, 'r', encoding='utf-8') as f_forbidden:
                for line in f_forbidden:
                    forbidden_ids.add(line.strip())
            print(f"Loaded {len(forbidden_ids)} forbidden novel IDs from {FORBIDDEN_FILE}.")
        except Exception as e:
            print(f"Error loading forbidden file {FORBIDDEN_FILE}: {e}", file=sys.stderr)

    # Initialize output file and indexed IDs based on configuration
    if config['output_file']:
        if config['continue_scrape']: # Append mode
            f_output = open(config['output_file'], 'a', encoding='utf-8')
            print(f"Appending to existing file: {config['output_file']}")
            with open(config['output_file'], 'r', encoding='utf-8') as f_read:
                for line in f_read:
                    try:
                        if config['scrape_metadata']:
                            data = json.loads(line)
                            if 'id' in data: indexed_ids.add(data['id'])
                        else: # scrape_titles_only
                            # Handle both normal and skipped novel format in TXT for re-indexing
                            match = re.search(r', (\d{6})\n?$', line) # For normal titles
                            if not match:
                                match = re.search(r'ID: (\d{6})', line) # For skipped status
                            if match: indexed_ids.add(match.group(1))
                    except json.JSONDecodeError:
                        print(f"Warning: Could not parse line in {config['output_file']}: {line.strip()}", file=sys.stderr)
                    except Exception as e:
                        print(f"Error reading existing file line: {e}", file=sys.stderr)
            print(f"Found {len(indexed_ids)} already indexed novels. These will be skipped.")
        else: # Overwrite mode
            f_output = open(config['output_file'], 'w', encoding='utf-8')
            print(f"Creating/overwriting output file: {config['output_file']}")
    else:
        print("Running in 'Download covers only' mode. No metadata/title files will be updated.")

    # Calculate initial size of existing covers if download_covers is enabled
    if config['download_covers']:
        for root, _, files in os.walk(DOWNLOAD_COVERS_FOLDER):
            for file in files:
                try:
                    current_download_size_bytes[0] += os.path.getsize(os.path.join(root, file))
                except OSError:
                    pass # Ignore files that might be inaccessible
        print(f"Initial cover folder size: {current_download_size_bytes[0] / (1024*1024):.2f} MB\n")


    # headers are no longer defined here as they are dynamically chosen per request
    async with aiohttp.ClientSession() as session: # Session created here
        tasks = [] # Tasks list initialized inside the session context
        for i in range(config['start_id'], config['end_id'] + 1):
            novel_id_str = f"{i:06d}"

            if novel_id_str in indexed_ids: # Skip if already processed and in append mode
                continue
            # Skip if in forbidden list, UNLESS ignore_forbidden_file is True
            if novel_id_str in forbidden_ids and not config['ignore_forbidden_file']:
                continue

            tasks.append(
                asyncio.create_task(
                    process_novel(
                        session, novel_id_str, semaphore, f_output,
                        config['scrape_metadata'], config['scrape_titles_only'],
                        config['download_covers'],
                        current_download_size_bytes, config['max_storage_bytes'],
                        forbidden_ids, config['min_delay'], config['max_delay'],
                        config['scrape_skipped_novels'] # Pass new flag
                    )
                )
            )
        total_tasks_created = len(tasks)
        if not tasks:
            print("\nNo new novels to process in the selected range. Exiting.")
            if f_output: f_output.close()
            return


        print(f"Found {total_tasks_created} new novels to process.")
        try:
            for task in asyncio.as_completed(tasks):
                try:
                    novel_id, result_status, cover_downloaded, data_written = await task
                except asyncio.CancelledError:
                    continue
                except IPBanException as e:
                    print(f"\n\n🚨 {e}", file=sys.stderr)
                    print("Terminating scrape due to suspected IP ban.", file=sys.stderr)
                    # Do not cancel other tasks here, let them finish if they can
                    # for t in tasks: t.cancel() # Removed this line
                    # break # Removed this line
                    # Instead, just log and allow the loop to continue for other completed tasks
                    continue # Continue to the next completed task

                tasks_completed += 1
                if total_tasks_created > 0:
                    progress_percent = (tasks_completed / total_tasks_created) * 100
                    status_msg = f"Processed ID: {novel_id} -> '{result_status}'"
                    progress_msg = f"Progress: {tasks_completed}/{total_tasks_created} ({progress_percent:.2f}%)"
                    print(f"{status_msg} | {progress_msg}")
                    sys.stdout.flush()

                if result_status == 'latest_novel_reached':
                    print(f"\n\n🏁 Reached last known novel, {novel_id} - 잘못된 소설 번호 입니다.")
                    # Do not cancel other tasks here, let them finish if they can
                    # for t in tasks: t.cancel() # Removed this line
                    # break # Removed this line
                    # The loop will naturally complete all initiated tasks
                    continue # Continue to the next completed task

                if cover_downloaded: covers_downloaded += 1
                if data_written: found_count += 1
        finally:
            if f_output:
                f_output.close()
            print("\n\nScraping complete!")
            print(f"Total novel pages attempted: {total_tasks_created}")
            print(f"Total data entries written to file: {found_count}")
            print(f"Total covers downloaded or already existed: {covers_downloaded}")
            print(f"Total cover storage used: {current_download_size_bytes[0] / (1024*1024):.2f} MB")
            print(f"Total time taken: {time.time() - start_time:.2f} seconds")


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
            print(f"\nAn unexpected error occurred in main execution: {e}", file=sys.stderr)
            # Optional: print traceback for more detailed error info
            # import traceback
            # traceback.print_exc()
    input("\nScript finished. Press Enter to exit.") # Added this line