import asyncio
import aiohttp
from bs4 import BeautifulSoup
import re
import time
import os
import sys
import json
import datetime
import subprocess
import importlib.util
from io import BytesIO
from PIL import Image
import exifread
import random

# --- Automatic Dependency Installation Check ---
def check_dependencies():
    """Checks for required packages and prompts for installation if missing."""
    # Mapping of package names (for pip) to their import names (for Python)
    required_packages = {
        "requests": "requests",
        "beautifulsoup4": "bs4",
        "aiohttp": "aiohttp",
        "Pillow": "PIL",
        "exifread": "exifread"
    }
    
    missing_packages = [
        pkg_name for pkg_name, import_name in required_packages.items() 
        if not importlib.util.find_spec(import_name)
    ]

    if missing_packages:
        print(f"Detected missing dependencies: {', '.join(missing_packages)}. Attempting to install...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", *missing_packages])
            print("Dependencies installed successfully. Please restart the script.")
            sys.exit(0)
        except (subprocess.CalledProcessError, Exception) as e:
            print(f"ERROR: Failed to install dependencies automatically: {e}", file=sys.stderr)
            print(f"Please manually install them by running: pip install {' '.join(missing_packages)}", file=sys.stderr)
            sys.exit(1)

# --- Custom Logger ---
class Logger(object):
    """Writes console output to both the terminal and a log file."""
    def __init__(self, filename="log.txt"):
        self.terminal = sys.stdout
        self.log_file_path = filename
        # Clear log file on start
        with open(filename, "w", encoding="utf-8") as f:
            f.write(f"--- Log for session started: {datetime.datetime.now()} ---\n\n")
        self.log = open(filename, "a", encoding="utf-8")

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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.log.write(f"\n--- Log for session ended: {datetime.datetime.now()} ---\n")
        self.log.close()
        sys.stdout = self.terminal
        sys.stderr = self.terminal

# --- Configuration ---
DEFAULT_END_ID = 999999
OUTPUT_FILE_TITLES = "novelpia_titles.txt"
OUTPUT_FILE_METADATA = "novelpia_metadata.jsonl"
DOWNLOAD_COVERS_FOLDER = "novelpia_covers"
FORBIDDEN_FILE = "forbidden.txt"
CONCURRENT_REQUESTS_LIMIT = 5

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/122.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

class IPBanException(Exception):
    """Custom exception for suspected IP bans."""
    pass

# --- Core Functions ---

async def fetch_page(session, novel_id_str, semaphore, min_delay, max_delay):
    """Fetches HTML content for a novel, with retry logic and random delays."""
    url = f"https://novelpia.com/novel/{novel_id_str}"
    await asyncio.sleep(random.uniform(min_delay, max_delay))
    headers = {"User-Agent": random.choice(USER_AGENTS), "Referer": "https://novelpia.com/"}

    async with semaphore:
        try:
            # First Attempt
            async with session.get(url, headers=headers, timeout=15) as response:
                response.raise_for_status()
                html = await response.text()
                if html and html.strip():
                    return html

            # First attempt failed (blank page), retry after 5s
            print(f"\nWarning: Blank page for {novel_id_str}. Retrying in 5s...", file=sys.stderr)
            await asyncio.sleep(5)

            # Second Attempt
            async with session.get(url, headers=headers, timeout=15) as response:
                response.raise_for_status()
                html = await response.text()
                if html and html.strip():
                    return html
            
            # If both attempts fail with blank pages, assume rate-limiting and pause
            print("\n\n" + "#"*80, file=sys.stderr)
            print("!! WARNING: POSSIBLE IP BAN DETECTED !!".center(80), file=sys.stderr)
            print("Received a blank page twice. Pausing for 24 hours.".center(80), file=sys.stderr)
            print(f"Pausing at {datetime.datetime.now()}. Will resume at {datetime.datetime.now() + datetime.timedelta(hours=24)}.".center(80), file=sys.stderr)
            print("#"*80 + "\n", file=sys.stderr)
            await asyncio.sleep(24 * 60 * 60)

            # Final Attempt after long pause
            print(f"\nResuming scrape. Final attempt for ID {novel_id_str}...", file=sys.stderr)
            async with session.get(url, headers=headers, timeout=30) as response:
                response.raise_for_status()
                html = await response.text()
                if html and html.strip():
                    return html

            raise IPBanException(f"Suspected IP Ban at novel ID {novel_id_str}")

        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"Network/Timeout Error fetching {url}: {e}", file=sys.stderr)
            return None
        except Exception as e:
            if not isinstance(e, IPBanException):
                print(f"Unexpected error fetching {url}: {e}", file=sys.stderr)
            raise

async def download_cover(session, url, local_path, current_download_size_bytes_ref, max_storage_bytes, min_delay, max_delay):
    """Downloads and saves a novel cover image."""
    await asyncio.sleep(random.uniform(min_delay, max_delay))
    headers = {"User-Agent": random.choice(USER_AGENTS), "Referer": "https://novelpia.com/"}

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
            except Exception as e:
                print(f"Error processing image {url}, writing raw: {e}", file=sys.stderr)
                with open(local_path, 'wb') as f:
                    f.write(content)
            
            file_size = os.path.getsize(local_path)
            current_download_size_bytes_ref[0] += file_size
            return local_path
    except Exception as e:
        print(f"Error downloading cover {url}: {e}", file=sys.stderr)
        return "DOWNLOAD_FAILED_UNKNOWN"

def parse_novel_data(html_content, novel_id_str):
    """Parses HTML to extract novel metadata."""
    if not html_content: return None
    soup = BeautifulSoup(html_content, 'html.parser')

    alert_modal_div = soup.find('div', id='alert_modal', class_='modal')
    if alert_modal_div:
        modal_text = alert_modal_div.get_text(strip=True)
        if "잘못된 소설 번호 입니다." in modal_text: return 'LATEST_NOVEL_REACHED'
        if "삭제된 소설 입니다." in modal_text: return {"id": novel_id_str, "status": "deleted_novel", "title": "DELETED NOVEL", "publication_status": "삭제됨"}
        if "잘못된 접근입니다." in modal_text: return {"id": novel_id_str, "status": "access_denied_novel", "title": "ACCESS DENIED NOVEL", "publication_status": "접근불가"}

    title_tag = soup.find('meta', attrs={'name': 'twitter:title'})
    title = re.search(r'노벨피아 - 웹소설로 꿈꾸는 세상! - (.+)', title_tag['content']).group(1).strip() if title_tag else None
    if not title: return None

    synopsis_tag = soup.find('meta', attrs={'name': 'twitter:description'})
    author_tag = soup.find('a', class_='writer-name')
    tags_container = soup.find('p', class_='writer-tag')
    
    like_count, chapter_count = None, None
    info_div = soup.find('div', class_='info-count2')
    if info_div:
        for p in info_div.find_all('p'):
            text = p.get_text(strip=True)
            num_str_match = re.search(r'(\d{1,3}(?:,\d{3})*)', text)
            if num_str_match:
                num = int(num_str_match.group(1).replace(',', ''))
                if '선호' in text: like_count = num
                elif '회차' in text: chapter_count = num

    cover_url, cover_mime_type = None, None
    og_image_tag = soup.find('meta', attrs={'property': 'og:image'})
    if og_image_tag and 'content' in og_image_tag.attrs:
        extracted_url = og_image_tag['content']
        # Skip known placeholder/default images
        if not ("novelpia.com/img/" in extracted_url and "2025-novelpia" in extracted_url):
            cover_url = extracted_url
            og_image_type_tag = soup.find('meta', attrs={'property': 'og:image:type'})
            if og_image_type_tag and 'content' in og_image_type_tag.attrs:
                cover_mime_type = og_image_type_tag['content'].strip()

    return {
        "id": novel_id_str, "title": title,
        "synopsis": synopsis_tag['content'].strip() if synopsis_tag else None,
        "author": author_tag.get_text(strip=True) if author_tag else None,
        "tags": [span.get_text(strip=True) for span in tags_container.find_all('span', class_='tag') if span.get_text(strip=True) != '+나만의태그 추가'] if tags_container else [],
        "is_adult": bool(soup.find('span', class_='b_19 s_inv', string='19')),
        "publication_status": "완결" if soup.find('span', class_='b_comp s_inv', string='완결') else "연재중단" if soup.find('span', class_='s_inv', string='연재중단') else "연재중",
        "cover_url": cover_url, "cover_mime_type": cover_mime_type, "cover_local_path": None,
        "like_count": like_count, "chapter_count": chapter_count
    }

async def process_novel(session, novel_id_str, semaphore, file_handle, config, current_download_size_bytes_ref, forbidden_novel_ids_set):
    """Fetches, parses, and writes data for a single novel."""
    html_content = await fetch_page(session, novel_id_str, semaphore, config['min_delay'], config['max_delay'])
    if html_content is None:
        return novel_id_str, 'network_error', False, False

    data = parse_novel_data(html_content, novel_id_str)
    if data == 'LATEST_NOVEL_REACHED':
        return novel_id_str, 'latest_novel_reached', False, False

    cover_downloaded, data_written = False, False
    status = 'skipped_no_data'

    if isinstance(data, dict):
        status = data.get('status', 'found')
        if status in ["deleted_novel", "access_denied_novel"] and not config['scrape_skipped_novels']:
            if novel_id_str not in forbidden_novel_ids_set:
                forbidden_novel_ids_set.add(novel_id_str)
                with open(FORBIDDEN_FILE, 'a', encoding='utf-8') as f:
                    f.write(novel_id_str + '\n')
            return novel_id_str, 'skipped_forbidden', False, False

        if config['download_covers'] and data.get('cover_url'):
            if data.get('is_adult') and not config.get('download_adult_covers'):
                data['cover_local_path'] = "SKIPPED_ADULT"
            else:
                # Determine file extension
                ext = ".jpg" # Default
                mime_map = {'image/png': '.png', 'image/jpeg': '.jpg', 'image/gif': '.gif', 'image/webp': '.webp'}
                if data.get('cover_mime_type') in mime_map:
                    ext = mime_map[data['cover_mime_type']]
                else:
                    url_path = data['cover_url'].split('?')[0]
                    url_ext = os.path.splitext(url_path)[1]
                    if url_ext in mime_map.values():
                        ext = url_ext

                cover_filename = f"{novel_id_str}{ext}"
                local_path = os.path.join(DOWNLOAD_COVERS_FOLDER, cover_filename)

                if os.path.exists(local_path) and not config.get('rescrape'):
                    data['cover_local_path'] = local_path
                    cover_downloaded = True
                else:
                    dl_status = await download_cover(session, data['cover_url'], local_path, current_download_size_bytes_ref, config['max_storage_bytes'], config['min_delay'], config['max_delay'])
                    data['cover_local_path'] = dl_status
                    if dl_status == local_path:
                        cover_downloaded = True

        if file_handle:
            if config['scrape_metadata']:
                file_handle.write(json.dumps(data, ensure_ascii=False) + '\n')
                data_written = True
            elif config['scrape_titles_only']:
                file_handle.write(f"{data.get('title', 'NO TITLE')}, {data['id']}\n")
                data_written = True

    return novel_id_str, status, cover_downloaded, data_written

def get_last_scraped_id(output_file, is_metadata_file):
    """Reads the last novel ID from an output file to allow resuming."""
    last_id = -1
    if not os.path.exists(output_file): return last_id
    try:
        with open(output_file, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    if is_metadata_file:
                        current_id = int(json.loads(line).get('id', -1))
                    else:
                        match = re.search(r', (\d+)\n?$', line) or re.search(r'ID: (\d+)', line)
                        current_id = int(match.group(1)) if match else -1
                    if current_id > last_id:
                        last_id = current_id
                except (json.JSONDecodeError, ValueError, AttributeError):
                    continue
    except Exception as e:
        print(f"Error reading {output_file}: {e}", file=sys.stderr)
    return last_id

def get_ids_for_rescrape(metadata_file, skip_completed):
    """Reads novel IDs from the metadata file for the 'rescrape' mode."""
    ids = []
    if not os.path.exists(metadata_file):
        print(f"Error: Metadata file '{metadata_file}' not found.", file=sys.stderr)
        return ids
    
    with open(metadata_file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                data = json.loads(line)
                if skip_completed and data.get('publication_status') == '완결':
                    continue
                if 'id' in data:
                    ids.append(data['id'])
            except (json.JSONDecodeError, KeyError):
                print(f"Warning: Skipping malformed line in metadata file: {line.strip()}", file=sys.stderr)
    return ids

def _get_id_range_from_user():
    """Prompts user for a novel ID range."""
    while True:
        range_input = input(f"➡️ Enter novel ID range (e.g., 0-{DEFAULT_END_ID}): ").strip()
        match = re.match(r'^\s*(\d+)\s*-\s*(\d+)\s*$', range_input)
        if match:
            start, end = int(match.group(1)), int(match.group(2))
            return min(start, end), max(start, end)
        print("Error: Invalid range format. Please use 'START-END'.")

def configure_scrape():
    """Gets user input to configure the scraping session."""
    config = {'output_file': None, 'start_id': 0, 'end_id': DEFAULT_END_ID, 'max_storage_bytes': 0,
              'scrape_metadata': False, 'scrape_titles_only': False, 'download_covers': False, 'continue_scrape': False,
              'min_delay': 0.5, 'max_delay': 1.5, 'ignore_forbidden_file': False, 'scrape_skipped_novels': False,
              'rescrape': False, 'skip_completed_on_rescrape': False, 'download_adult_covers': False}

    while True:
        choice = input("What do you want to do?\n  1. Scrape full metadata (JSONL)\n  2. Scrape titles only (TXT)\n  3. Download cover images only\n  4. Rescrape and update existing metadata\nEnter choice (1/2/3/4): ").strip()
        if choice == '1': config.update({'scrape_metadata': True, 'output_file': OUTPUT_FILE_METADATA}); break
        elif choice == '2': config.update({'scrape_titles_only': True, 'output_file': OUTPUT_FILE_TITLES}); break
        elif choice == '3': config.update({'download_covers': True}); break
        elif choice == '4':
            if not os.path.exists(OUTPUT_FILE_METADATA):
                print(f"\nError: '{OUTPUT_FILE_METADATA}' not found. Cannot use rescrape option.")
                continue
            config.update({'rescrape': True, 'scrape_metadata': True, 'output_file': OUTPUT_FILE_METADATA})
            if input("Skip rescraping 'completed' novels? (y/n): ").lower().strip() == 'y':
                config['skip_completed_on_rescrape'] = True
            break
        else: print("Invalid choice.")

    if not config['rescrape']:
        if config['output_file']:
            last_id = get_last_scraped_id(config['output_file'], config['scrape_metadata'])
            if last_id != -1:
                if input(f"\nPrevious session found. Continue from last novel (ID {last_id + 1})? (y/n): ").lower().strip() == 'y':
                    config.update({'start_id': last_id + 1, 'continue_scrape': True})
                else:
                    config['start_id'], config['end_id'] = _get_id_range_from_user()
                    if input("Overwrite existing file? (y/n): ").lower().strip() != 'y':
                        config['continue_scrape'] = True
            else:
                config['start_id'], config['end_id'] = _get_id_range_from_user()
        elif config['download_covers']:
            config['start_id'], config['end_id'] = _get_id_range_from_user()

    if not config.get('download_covers') and input("\nDownload cover images as well? (y/n): ").lower().strip() == 'y':
        config['download_covers'] = True

    if config.get('download_covers'):
        os.makedirs(DOWNLOAD_COVERS_FOLDER, exist_ok=True)
        config['download_adult_covers'] = input("Download covers for adult (R19) novels? (y/n): ").lower().strip() == 'y'
        while True:
            try:
                storage_limit_gb = float(input("Enter max storage for covers in GB (e.g., 5.0): "))
                config['max_storage_bytes'] = storage_limit_gb * 1024**3
                break
            except ValueError: print("Invalid number.")

    if input("Ignore 'forbidden.txt' file? (y/n): ").lower().strip() == 'y':
        config['ignore_forbidden_file'] = True

    if config['scrape_metadata']:
        if input("Scrape 'deleted'/'access denied' novels? (y/n): ").lower().strip() == 'y':
            config['scrape_skipped_novels'] = True
    return config

async def main(config):
    """Main function to orchestrate the scraping process."""
    start_time = time.time()
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
    current_download_size_bytes = [0]
    forbidden_ids = set()
    
    if os.path.exists(FORBIDDEN_FILE) and not config['ignore_forbidden_file']:
        with open(FORBIDDEN_FILE, 'r', encoding='utf-8') as f:
            forbidden_ids.update(line.strip() for line in f)
        print(f"Loaded {len(forbidden_ids)} forbidden IDs.")

    if config['download_covers']:
        current_download_size_bytes[0] = sum(os.path.getsize(os.path.join(r, file)) for r, _, files in os.walk(DOWNLOAD_COVERS_FOLDER) for file in files if os.path.isfile(os.path.join(r, file)))
        print(f"Initial cover folder size: {current_download_size_bytes[0] / (1024*1024):.2f} MB")

    if config.get('rescrape'):
        await run_rescrape(config, semaphore, current_download_size_bytes, forbidden_ids, start_time)
    else:
        await run_normal_scrape(config, semaphore, current_download_size_bytes, forbidden_ids, start_time)

async def run_normal_scrape(config, semaphore, current_download_size_bytes, forbidden_ids, start_time):
    """Handles a standard, ranged scraping session."""
    print(f"Starting Novelpia scraping from ID {config['start_id']:06d} to {config['end_id']:06d}...")
    
    indexed_ids = set()
    f_output = None
    if config['output_file']:
        mode = 'a' if config['continue_scrape'] else 'w'
        f_output = open(config['output_file'], mode, encoding='utf-8')
        if config['continue_scrape'] and os.path.exists(config['output_file']):
            with open(config['output_file'], 'r', encoding='utf-8') as f_read:
                for line in f_read:
                    try:
                        if config['scrape_metadata']: indexed_ids.add(json.loads(line)['id'])
                        else: 
                            match = re.search(r', (\d+)', line) or re.search(r'ID: (\d+)', line)
                            if match: indexed_ids.add(match.group(1))
                    except (json.JSONDecodeError, KeyError): continue
            print(f"Found {len(indexed_ids)} already indexed novels to skip.")

    tasks = {}
    latest_known_novel_id = [float('inf')]
    
    id_range = range(config['start_id'], config['end_id'] + 1)
    tasks_to_create = [f"{i:06d}" for i in id_range if f"{i:06d}" not in indexed_ids and f"{i:06d}" not in forbidden_ids]

    if not tasks_to_create:
        print("\nNo new novels to process in the selected range.")
        if f_output: f_output.close()
        return

    print(f"Created {len(tasks_to_create)} new tasks.")
    found_count, covers_downloaded, tasks_completed = 0, 0, 0
    
    async with aiohttp.ClientSession() as session:
        for novel_id_str in tasks_to_create:
            task = asyncio.create_task(process_novel(session, novel_id_str, semaphore, f_output, config, current_download_size_bytes, forbidden_ids))
            tasks[novel_id_str] = task
        
        try:
            for future in asyncio.as_completed(tasks.values()):
                try:
                    novel_id, status, cover_dl, data_wr = await future
                except asyncio.CancelledError:
                    tasks_completed += 1 # Count cancelled tasks as completed for progress
                    continue
                except IPBanException as e:
                    print(f"\n\n🚨 {e}\nTerminating scrape due to suspected IP ban.", file=sys.stderr)
                    for t in tasks.values(): t.cancel()
                    break
                
                tasks_completed += 1
                progress = (tasks_completed / len(tasks_to_create)) * 100
                print(f"ID: {novel_id} -> '{status}' | Progress: {tasks_completed}/{len(tasks_to_create)} ({progress:.2f}%)", end='\r')

                if status == 'latest_novel_reached':
                    current_latest = int(novel_id)
                    if current_latest < latest_known_novel_id[0]:
                        latest_known_novel_id[0] = current_latest
                        print(f"\n--- Latest novel boundary found at {current_latest}. Cancelling tasks for higher IDs. ---")
                        for task_id, task_to_cancel in tasks.items():
                            if int(task_id) > current_latest and not task_to_cancel.done():
                                task_to_cancel.cancel()
                
                if int(novel_id) < latest_known_novel_id[0]:
                    if cover_dl: covers_downloaded += 1
                    if data_wr: found_count += 1
        finally:
            if f_output: f_output.close()
            print() # Newline after progress bar
            print_summary("Scraping", len(tasks_to_create), found_count, covers_downloaded, current_download_size_bytes[0], start_time)

async def run_rescrape(config, semaphore, current_download_size_bytes, forbidden_ids, start_time):
    """Handles rescraping and updating existing metadata."""
    print("--- Rescrape Mode ---")
    ids_to_process = get_ids_for_rescrape(config['output_file'], config['skip_completed_on_rescrape'])
    if not ids_to_process:
        print("No novels to rescrape based on criteria. Exiting.")
        return

    temp_output_file = config['output_file'] + '.tmp'
    f_output = open(temp_output_file, 'w', encoding='utf-8')
    success = False
    found_count, covers_downloaded, tasks_completed = 0, 0, 0

    tasks = {}
    
    print(f"Created {len(ids_to_process)} tasks for rescraping.")
    async with aiohttp.ClientSession() as session:
        for novel_id_str in ids_to_process:
            task = asyncio.create_task(process_novel(session, novel_id_str, semaphore, f_output, config, current_download_size_bytes, forbidden_ids))
            tasks[novel_id_str] = task
            
        try:
            for future in asyncio.as_completed(tasks.values()):
                try:
                    novel_id, status, cover_dl, data_wr = await future
                except asyncio.CancelledError:
                    continue
                except IPBanException as e:
                    print(f"\n\n🚨 {e}\nTerminating rescrape due to suspected IP ban.", file=sys.stderr)
                    for t in tasks.values(): t.cancel()
                    break
                
                tasks_completed += 1
                progress = (tasks_completed / len(ids_to_process)) * 100
                print(f"Rescraping ID: {novel_id} -> '{status}' | Progress: {tasks_completed}/{len(ids_to_process)} ({progress:.2f}%)", end='\r')

                if cover_dl: covers_downloaded += 1
                if data_wr: found_count += 1
            else:
                success = True
        finally:
            f_output.close()
            if success:
                os.replace(temp_output_file, config['output_file'])
                print(f"\nSuccessfully rescraped. '{config['output_file']}' has been updated.")
            else:
                os.remove(temp_output_file)
                print(f"\nRescrape failed or was interrupted. Original file '{config['output_file']}' is untouched.")
            print() # Newline after progress bar
            print_summary("Rescraping", len(ids_to_process), found_count, covers_downloaded, current_download_size_bytes[0], start_time)

def print_summary(mode, total_tasks, found, covers, storage_bytes, start_time):
    """Prints a summary at the end of a run."""
    print(f"\n\n{mode} complete!")
    print(f"Total novel pages attempted: {total_tasks}")
    print(f"Total data entries written/updated: {found}")
    print(f"Total covers downloaded/updated: {covers}")
    print(f"Total cover storage used: {storage_bytes / (1024*1024):.2f} MB")
    print(f"Total time taken: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    check_dependencies()
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
            import traceback
            traceback.print_exc()
    input("\nScript finished. Press Enter to exit.")
