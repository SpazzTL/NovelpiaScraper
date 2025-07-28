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

# --- Automatic Dependency Installation Check ---
required_packages = {
    "requests": "requests",
    "bs4": "beautifulsoup4", # Package name for pip is 'beautifulsoup4'
    "aiohttp": "aiohttp",
    "Pillow": "Pillow", # Added Pillow as it's used for image conversion
    "exifread": "exifread" # Added exifread for EXIF data parsing
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
        # Add --user for user-specific installation if permissions are an issue
        subprocess.check_call([sys.executable, "-m", "pip", "install", *missing_packages])
        print("Dependencies installed successfully. Please restart the script.")
        sys.exit(0) # Exit after installation, user should restart to ensure imports work
    except subprocess.CalledProcessError as e:
        print(f"ERROR: Failed to install dependencies automatically: {e}")
        print("Please ensure you have an active internet connection.")
        print("You might need to run the script from an administrator command prompt, or manually install:")
        print(f"  pip install {' '.join(missing_packages)}")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred during dependency installation: {e}")
        sys.exit(1)

# Now that we're sure all dependencies are installed, import them
# (Some might be imported above for the check, but re-importing ensures consistency)
import asyncio
import aiohttp
from bs4 import BeautifulSoup
from PIL import Image # Ensure Image is imported for cover conversion
import exifread # Ensure exifread is imported
import time

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
CONCURRENT_REQUESTS_LIMIT = 1
MAX_CONSECUTIVE_NETWORK_ERRORS_FOR_PROMPT = 100000
MAX_CONSECUTIVE_COVER_DOWNLOAD_ERRORS = 10

# --- Asynchronous HTTP Fetcher ---
async def fetch_page(session, novel_id_str, semaphore):
    """Fetches the HTML content of a given novel URL.
    Prints errors to stderr and returns None on failure.
    """
    url = f"https://novelpia.com/novel/{novel_id_str}"
    async with semaphore: # Acquire a semaphore slot before making a request
        try:
            async with session.get(url, timeout=10) as response:
                response.raise_for_status() # Raise an exception for HTTP errors (4xx or 5xx)
                return await response.text()
        except aiohttp.ClientError as e:
            print(f"Network Error fetching page {url}: {e}", file=sys.stderr)
            return None
        except asyncio.TimeoutError:
            print(f"Timeout fetching page {url}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"Unexpected error fetching page {url}: {e}", file=sys.stderr)
            return None

async def download_cover(session, url, local_path, current_download_size_bytes_ref, max_storage_bytes):
    """Downloads a cover image and saves it locally.
    Updates the shared download size reference.
    Returns local_path on success, or a status string on failure/skip.
    """
    if current_download_size_bytes_ref[0] >= max_storage_bytes:
        print(f"Storage limit reached. Skipping download for {url}", file=sys.stderr)
        return "SKIPPED_LIMIT"

    try:
        async with session.get(url, timeout=20) as response: # Longer timeout for image downloads
            response.raise_for_status() # This will raise for 404, etc.
            content = await response.read()
            
            # Check size before writing to ensure we don't exceed limit mid-download
            if current_download_size_bytes_ref[0] + len(content) > max_storage_bytes:
                print(f"Download of {url} would exceed storage limit. Skipping.", file=sys.stderr)
                return "SKIPPED_LIMIT"

            # Use Pillow to open and save the image to ensure consistent format (JPEG)
            # This also helps in handling potentially malformed images by re-encoding them.
            try:
                img = Image.open(BytesIO(content))
                if img.mode == 'RGBA': # Convert to RGB if it has an alpha channel
                    img = img.convert('RGB')
                
                # Save initially as JPEG, as this is the primary target format
                img.save(local_path, "JPEG", quality=85) 
                
                # --- EXIF Data Check and File Type Correction ---
                # Only attempt EXIF read if the file was saved as a JPEG
                if local_path.lower().endswith((".jpg", ".jpeg")):
                    with open(local_path, 'rb') as f:
                        tags = exifread.process_file(f)
                    
                    if 'Image FileTypeExtension' in tags:
                        exif_ext_tag = str(tags['Image FileTypeExtension']).lower()
                        # Remove leading dot if present, then add our own
                        exif_ext = "." + exif_ext_tag.lstrip('.') 
                        
                        current_ext = os.path.splitext(local_path)[1].lower()

                        if exif_ext != current_ext:
                            new_local_path = os.path.splitext(local_path)[0] + exif_ext
                            try:
                                os.rename(local_path, new_local_path)
                                print(f"Renamed cover from {os.path.basename(local_path)} to {os.path.basename(new_local_path)} based on EXIF.", file=sys.stderr)
                                local_path = new_local_path # Update local_path to the new path
                            except OSError as e:
                                print(f"Error renaming file {local_path} to {new_local_path}: {e}", file=sys.stderr)
                
            except Exception as e:
                print(f"Error processing image with Pillow or EXIF for {url}: {e}", file=sys.stderr)
                # Fallback to direct write if Pillow/EXIF fails (though less robust)
                with open(local_path, 'wb') as f:
                    f.write(content)

            file_size = os.path.getsize(local_path)
            current_download_size_bytes_ref[0] += file_size
            return local_path
    except aiohttp.ClientResponseError as e: # Catch specific HTTP errors like 404
        print(f"HTTP Error downloading cover {url}: {e.status} {e.message}", file=sys.stderr)
        return "DOWNLOAD_FAILED_HTTP_ERROR"
    except aiohttp.ClientError as e: # Catch other network-related client errors
        print(f"Network Error downloading cover {url}: {e}", file=sys.stderr)
        return "DOWNLOAD_FAILED_NETWORK_ERROR"
    except asyncio.TimeoutError:
        print(f"Timeout downloading cover {url}", file=sys.stderr)
        return "DOWNLOAD_FAILED_TIMEOUT"
    except Exception as e:
        print(f"Unexpected error downloading cover {url}: {e}", file=sys.stderr)
        return "DOWNLOAD_FAILED_UNKNOWN"

# --- HTML Parser for Metadata ---
def parse_novel_data(html_content, novel_id_str):
    """Parses the HTML content to extract novel title, synopsis, author, tags, age rating, publication status, cover URL, like count, and chapter count.
    Returns None if the page indicates a deleted novel or incorrect access.
    """
    if not html_content:
        return None

    soup = BeautifulSoup(html_content, 'html.parser')

    # Check for "deleted novel" or "incorrect access" indicator immediately
    # The specific div to look for is <div id="alert_modal" class="modal fade" ...>
    alert_modal_div = soup.find('div', id='alert_modal', class_='modal')
    if alert_modal_div:
        modal_text = alert_modal_div.get_text(strip=True)
        if "삭제된 소설 입니다." in modal_text:
            # print(f"Skipping novel {novel_id_str}: Detected as deleted novel.", file=sys.stderr)
            return None # Indicate should be skipped
        elif "잘못된 접근입니다." in modal_text:
            # print(f"Skipping novel {novel_id_str}: Detected as incorrect access.", file=sys.stderr)
            return None # Indicate should be skipped

    # 1. Extract Title
    title = None
    meta_title_tag = soup.find('meta', attrs={'name': 'twitter:title'})
    if meta_title_tag and 'content' in meta_title_tag.attrs:
        full_title = meta_title_tag['content']
        match = re.search(r'노벨피아 - 웹소설로 꿈꾸는 세상! - (.+)', full_title)
        if match:
            title = match.group(1).strip()

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
    like_count = None
    chapter_count = None

    info_count_div = soup.find('div', class_='info-count2')
    if info_count_div:
        # Find all <p> tags within the info-count2 div
        p_tags = info_count_div.find_all('p')
        for p_tag in p_tags:
            text = p_tag.get_text(strip=True)
            
            # Regex to find numbers (with optional commas)
            # This regex looks for one or more digits, optionally followed by commas and three digits
            # It's flexible enough to capture "123" or "123,456"
            number_match = re.search(r'(\d{1,3}(?:,\d{3})*)', text)
            if number_match:
                extracted_number_str = number_match.group(1).replace(',', '') # Remove commas
                try:
                    extracted_number = int(extracted_number_str)
                    
                    if '선호' in text: # "선호" means "likes"
                        like_count = extracted_number
                    elif '회차' in text: # "회차" means "chapters"
                        chapter_count = extracted_number
                except ValueError:
                    # This should ideally not happen if regex is good, but catch for safety
                    print(f"Warning: Could not convert '{extracted_number_str}' to int in '{text}'", file=sys.stderr)
                    pass


    # Only return data if a title was found, indicating a valid novel page
    if title:
        return {
            "id": novel_id_str,
            "title": title,
            "synopsis": synopsis,
            "author": author,
            "tags": tags,
            "is_adult": is_adult,
            "publication_status": publication_status,
            "cover_url": cover_url, # Add cover URL to metadata
            "cover_mime_type": cover_mime_type, # Add mime type
            "cover_local_path": None, # Placeholder for local path, will be filled later
            "like_count": like_count,
            "chapter_count": chapter_count
        }
    return None

# --- Main Scraper Logic ---
async def main():
    """Main function to orchestrate the scraping process."""
    processed_count = 0
    total_novel_pages_processed_with_data = 0 
    total_covers_downloaded = 0
    current_download_size_bytes = [0] # Use a list to pass by reference for mutable update
    start_time = time.time()
    semaphore = asyncio.Semaphore(CONCURRENT_REQUESTS_LIMIT)
    consecutive_network_errors = 0 # Counter for consecutive network errors fetching pages
    consecutive_cover_download_error_count = 0 # Counter for consecutive errors downloading covers

    # These global variables are updated by _get_id_range_from_user() before main() is called
    # So main() will use the user-defined or default range.
    global START_ID, END_ID 

    print(f"Starting Novelpia scraping from ID {START_ID:06d} to {END_ID:06d}...")
    print(f"Concurrent requests limit: {CONCURRENT_REQUESTS_LIMIT}")
    print(f"Maximum consecutive network errors before prompt: {MAX_CONSECUTIVE_NETWORK_ERRORS_FOR_PROMPT}")
    print(f"Maximum consecutive cover download errors before stopping: {MAX_CONSECUTIVE_COVER_DOWNLOAD_ERRORS}\n")

    # --- User Choice for Scraping Mode ---
    scrape_metadata = False
    scrape_titles_only = False
    download_covers_along_with_data = False
    download_covers_only = False
    max_storage_bytes = 0
    
    while True:
        print("What do you want to do?")
        print("  1. Scrape novel metadata (title, synopsis, author, tags, age, status) to JSONL.")
        print("  2. Scrape only novel titles to TXT.")
        print("  3. Download cover images only (no metadata/title files updated).")
        choice = input("Enter choice (1/2/3): ").strip()

        if choice == '1':
            scrape_metadata = True
            current_output_file = OUTPUT_FILE_METADATA
            break
        elif choice == '2':
            scrape_titles_only = True
            current_output_file = OUTPUT_FILE_TITLES
            break
        elif choice == '3':
            download_covers_only = True
            # No primary output file for this mode, just covers
            current_output_file = None 
            break
        else:
            print("Invalid choice. Please enter 1, 2, or 3.")

    # --- Handle Cover Download Options based on initial choice ---
    if scrape_metadata or scrape_titles_only: # If scraping data, ask about covers as an add-on
        while True:
            cover_choice = input("Do you want to download cover images along with the data? (y/n): ").lower().strip()
            if cover_choice == 'y':
                download_covers_along_with_data = True
                break
            elif cover_choice == 'n':
                break
            else:
                print("Invalid input. Please enter 'y' or 'n'.")
    
    if download_covers_along_with_data or download_covers_only:
        # Ask for storage limit if covers are to be downloaded in any mode
        while True:
            try:
                storage_limit_gb = float(input("Enter maximum storage limit for covers in GB (e.g., 5.0): "))
                max_storage_bytes = storage_limit_gb * 1024 * 1024 * 1024 # Convert GB to Bytes
                break
            except ValueError:
                print("Invalid input. Please enter a number for storage limit.")
        
        os.makedirs(DOWNLOAD_COVERS_FOLDER, exist_ok=True)
        print(f"Covers will be saved to: {DOWNLOAD_COVERS_FOLDER}")
        print(f"Maximum cover storage limit: {storage_limit_gb:.2f} GB\n")
        
        # Calculate initial size of existing covers in the folder
        for root, _, files in os.walk(DOWNLOAD_COVERS_FOLDER):
            for file in files:
                try:
                    current_download_size_bytes[0] += os.path.getsize(os.path.join(root, file))
                except OSError:
                    pass # Ignore files that might be inaccessible
        print(f"Initial cover folder size: {current_download_size_bytes[0] / (1024*1024):.2f} MB\n")

    # --- Handle output file and re-indexing for data scraping modes ---
    indexed_novel_ids = set()
    f_output = None
    forbidden_novel_ids = set()

    # Load forbidden IDs from file
    if os.path.exists(FORBIDDEN_FILE):
        try:
            with open(FORBIDDEN_FILE, 'r', encoding='utf-8') as f_forbidden:
                for line in f_forbidden:
                    forbidden_novel_ids.add(line.strip())
            print(f"Loaded {len(forbidden_novel_ids)} forbidden novel IDs from {FORBIDDEN_FILE}.")
        except Exception as e:
            print(f"Error loading forbidden file {FORBIDDEN_FILE}: {e}", file=sys.stderr)

    if current_output_file: # Only if a primary output file is used (Mode 1 or 2)
        print(f"Output will be saved to: {current_output_file}\n")
        if os.path.exists(current_output_file):
            while True:
                user_choice = input(f"Output file '{current_output_file}' already exists. Do you want to re-index all novels (y/n)? ").lower().strip()
                if user_choice == 'y':
                    print("Re-indexing all novels. Existing file will be overwritten.")
                    f_output = open(current_output_file, 'w', encoding='utf-8')
                    break
                elif user_choice == 'n':
                    print("Skipping already indexed novels.")
                    f_output = open(current_output_file, 'a', encoding='utf-8')
                    with open(current_output_file, 'r', encoding='utf-8') as f_read:
                        for line in f_read:
                            try:
                                if scrape_metadata: # If JSONL, parse JSON
                                    data = json.loads(line)
                                    if 'id' in data:
                                        indexed_novel_ids.add(data['id'])
                                else: # If TXT, use regex
                                    match = re.search(r', (\d{6})\n?$', line)
                                    if match:
                                        indexed_novel_ids.add(match.group(1))
                            except json.JSONDecodeError:
                                print(f"Warning: Could not parse line in {current_output_file}: {line.strip()}", file=sys.stderr)
                            except Exception as e:
                                print(f"Error reading existing file line: {e}", file=sys.stderr)

                    print(f"Found {len(indexed_novel_ids)} already indexed novels. These will be skipped.")
                    # Initialize counts with already indexed novels for accurate progress
                    processed_count = len(indexed_novel_ids)
                    total_novel_pages_processed_with_data = len(indexed_novel_ids)
                    break
                else:
                    print("Invalid input. Please enter 'y' or 'n'.")
        else:
            print(f"Creating new output file: {current_output_file}")
            f_output = open(current_output_file, 'w', encoding='utf-8')
    else: # Covers only mode, no primary output file
        print("Running in 'Download covers only' mode. No metadata/title files will be updated.")


    total_novels_in_range = END_ID - START_ID + 1 # Total possible novels to iterate over

    try:
        # Initialize aiohttp ClientSession here, so it's available for task creation
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://novelpia.com/" # Referer to mimic browser navigation
        }
        async with aiohttp.ClientSession(headers=headers) as session:
            tasks = []
            for i in range(START_ID, END_ID + 1):
                novel_id_str = f"{i:06d}" # Format as 000000, 000001, etc.
                
                # Skip if already indexed in the primary data file (Mode 1 or 2)
                # This check only applies if a primary output file is being used.
                if current_output_file and novel_id_str in indexed_novel_ids:
                    # If skipping, this ID is already counted in initial processed_count and total_novel_pages_processed_with_data
                    processed_count += 1 # Still count as processed for progress bar
                    continue 
                
                # Skip if forbidden
                if novel_id_str in forbidden_novel_ids:
                    # print(f"Skipping novel {novel_id_str}: Found in forbidden list.")
                    processed_count += 1 # Still count as processed for progress bar
                    continue

                tasks.append(
                    asyncio.create_task(
                        process_novel(
                            session, novel_id_str, semaphore, f_output, 
                            scrape_metadata, scrape_titles_only, 
                            download_covers_along_with_data or download_covers_only, 
                            current_download_size_bytes, max_storage_bytes,
                            forbidden_novel_ids # Pass forbidden_novel_ids set to process_novel
                        )
                    )
                )

            # Process tasks as they complete
            for task in asyncio.as_completed(tasks):
                # result will be a tuple: (status, cover_downloaded_flag, data_written_flag)
                result_status, cover_downloaded_flag, data_written_flag = await task 
                
                if cover_downloaded_flag:
                    total_covers_downloaded += 1
                
                if data_written_flag:
                    total_novel_pages_processed_with_data += 1

                # Handle page fetch network errors (for rate limiting check)
                if result_status == 'network_error':
                    consecutive_network_errors += 1
                    if consecutive_network_errors >= MAX_CONSECUTIVE_NETWORK_ERRORS_FOR_PROMPT:
                        print(f"\nLikely Rate Limited! ( {MAX_CONSECUTIVE_NETWORK_ERRORS_FOR_PROMPT} fails in a row )", file=sys.stderr)
                        user_input = input("Continue ? (Y/N): ").lower().strip()
                        if user_input == 'y':
                            consecutive_network_errors = 0 # Reset counter to continue
                            print("Continuing scrape...")
                        else:
                            print("Stopping scrape as requested.")
                            # Cancel remaining tasks for a cleaner exit
                            for t in tasks:
                                if not t.done():
                                    t.cancel()
                            return # Exit main function immediately
                else: 
                    consecutive_network_errors = 0 # Reset error count on success or non-network-error

                # Handle cover download errors (if applicable)
                if result_status in ["DOWNLOAD_FAILED_HTTP_ERROR", "DOWNLOAD_FAILED_NETWORK_ERROR", "DOWNLOAD_FAILED_TIMEOUT", "DOWNLOAD_FAILED_UNKNOWN"]:
                    consecutive_cover_download_error_count += 1
                    if consecutive_cover_download_error_count >= MAX_CONSECUTIVE_COVER_DOWNLOAD_ERRORS:
                        print(f"\n\nStopping due to {MAX_CONSECUTIVE_COVER_DOWNLOAD_ERRORS} consecutive cover download errors. Exiting.", file=sys.stderr)
                        for t in tasks:
                            if not t.done():
                                t.cancel()
                        return # Exit main function immediately
                else:
                    consecutive_cover_download_error_count = 0 # Reset cover error count on success or non-cover-related issue

                processed_count += 1 # Increment for each task that finishes

                elapsed_time = time.time() - start_time
                
                # Update progress every 100 novels or at the very end
                if processed_count % 100 == 0 or processed_count == total_novels_in_range:
                    progress_percent = (processed_count / total_novels_in_range) * 100
                    
                    eta_str = "N/A"
                    avg_time_per_novel_str = "N/A"
                    if processed_count > 0 and elapsed_time > 0:
                        avg_time_per_novel = elapsed_time / processed_count
                        avg_time_per_novel_str = f"{avg_time_per_novel:.4f}s"
                        remaining_novels = total_novels_in_range - processed_count
                        eta_seconds = remaining_novels * avg_time_per_novel
                        eta_str = time.strftime("%Hh %Mm %Ss", time.gmtime(eta_seconds))

                    sys.stdout.write(
                        f"\rProgress: {processed_count}/{total_novels_in_range} ({progress_percent:.2f}%) "
                        f"| Data Found: {total_novel_pages_processed_with_data} | Covers Downloaded: {total_covers_downloaded} ({current_download_size_bytes[0] / (1024*1024):.2f} MB) "
                        f"| Elapsed: {time.strftime('%Hh %Mm %Ss', time.gmtime(elapsed_time))} "
                        f"| Avg Time/Novel: {avg_time_per_novel_str} | ETA: {eta_str}"
                    )
                    sys.stdout.flush()

    finally:
        if f_output: # Ensure the file handle was successfully opened
            f_output.close()
        print("\n\nScraping complete!")
        print(f"Total novel pages processed: {processed_count}")
        print(f"Total data entries written to file: {total_novel_pages_processed_with_data}")
        print(f"Total covers downloaded: {total_covers_downloaded}")
        print(f"Total cover storage used: {current_download_size_bytes[0] / (1024*1024):.2f} MB")
        print(f"Total time taken: {time.time() - start_time:.2f} seconds")

async def process_novel(session, novel_id_str, semaphore, file_handle, 
                        scrape_metadata_flag, scrape_titles_only_flag, 
                        download_covers_flag, 
                        current_download_size_bytes_ref, max_storage_bytes,
                        forbidden_novel_ids_set): # New argument for forbidden IDs
    """Fetches, parses, and writes a single novel's data, and optionally downloads its cover.
    Returns a tuple: (status, cover_downloaded_flag, data_written_flag)
    """
    html_content = await fetch_page(session, novel_id_str, semaphore)
    if html_content is None:
        return 'network_error', False, False # Indicate a network-related error, no cover, no data

    novel_data = parse_novel_data(html_content, novel_id_str)
    cover_downloaded_this_novel = False
    data_written_this_novel = False
    
    # Default status if no data is found or processed
    status = 'skipped_no_data' 

    if novel_data:
        status = 'found' # Data was successfully parsed from the page
    else:
        # If novel_data is None, it means it was skipped by parse_novel_data (deleted/inaccessible)
        # Add to forbidden list if not already there
        if novel_id_str not in forbidden_novel_ids_set:
            forbidden_novel_ids_set.add(novel_id_str)
            with open(FORBIDDEN_FILE, 'a', encoding='utf-8') as f_forbidden:
                f_forbidden.write(novel_id_str + '\n')
            # print(f"Added novel {novel_id_str} to forbidden list.", file=sys.stderr)
        return 'skipped_forbidden', False, False # Indicate it was skipped due to being forbidden

    # Handle cover download logic
    if download_covers_flag and novel_data['cover_url']:
        if novel_data['is_adult']:
            novel_data['cover_local_path'] = "SKIPPED_ADULT"
        else:
            # Determine the correct file extension from the URL
            url_path = novel_data['cover_url'].split('?')[0] # Remove query parameters if any
            # Extract the last part after the dot, default to .jpg if no clear extension
            # This ensures we save it with the actual extension from the URL
            file_extension = os.path.splitext(url_path)[1]
            if not file_extension: # If no extension found, default to .jpg
                file_extension = ".jpg"
            # Ensure it's a valid image extension, otherwise default to .jpg
            if file_extension.lower() not in ['.png', '.jpg', '.jpeg', '.gif', '.webp']:
                file_extension = ".jpg" # Default to JPG if unknown/invalid

            cover_filename = f"{novel_id_str}{file_extension}"
            local_cover_path = os.path.join(DOWNLOAD_COVERS_FOLDER, cover_filename)

            if os.path.exists(local_cover_path):
                novel_data['cover_local_path'] = local_cover_path
                cover_downloaded_this_novel = True # Count as "available" cover
            elif current_download_size_bytes_ref[0] >= max_storage_bytes:
                novel_data['cover_local_path'] = "SKIPPED_LIMIT"
            else:
                download_status = await download_cover(
                    session, novel_data['cover_url'], local_cover_path, 
                    current_download_size_bytes_ref, max_storage_bytes
                )
                novel_data['cover_local_path'] = download_status
                if download_status == local_cover_path:
                    cover_downloaded_this_novel = True
                elif "DOWNLOAD_FAILED" in download_status:
                    # If cover download failed, update the status to reflect this
                    status = download_status 
    
    # Handle data writing logic
    # Only write if a file handle is provided (i.e., not in covers-only mode where file_handle is None)
    if file_handle: 
        if scrape_metadata_flag:
            # Write as JSON Line
            file_handle.write(json.dumps(novel_data, ensure_ascii=False) + '\n')
            data_written_this_novel = True
        elif scrape_titles_only_flag:
            # Write as plain text title, ID
            file_handle.write(f"{novel_data['title']}, {novel_data['id']}\n")
            data_written_this_novel = True
    
    return status, cover_downloaded_this_novel, data_written_this_novel

def _get_id_range_from_user():
    """
    Prompts the user for a novel ID range (e.g., "1-100").
    Handles default values and input validation.
    Returns (start_id, end_id) tuple.
    """
    while True:
        range_input = input(f"Enter novel ID range (e.g., 1-100) or press Enter for default ({DEFAULT_START_ID}-{DEFAULT_END_ID}): ").strip()
        
        if not range_input:
            print(f"Using default range: {DEFAULT_START_ID}-{DEFAULT_END_ID}")
            return DEFAULT_START_ID, DEFAULT_END_ID
        
        match = re.match(r'^\s*(\d+)\s*-\s*(\d+)\s*$', range_input)
        if match:
            try:
                start_id = int(match.group(1))
                end_id = int(match.group(2))
                
                if start_id > end_id:
                    print(f"Warning: Start ID ({start_id}) is greater than End ID ({end_id}). Swapping them.")
                    start_id, end_id = end_id, start_id
                
                # Ensure IDs are positive
                if start_id < 0 or end_id < 0:
                    print("Error: IDs must be positive integers.")
                    continue

                return start_id, end_id
            except ValueError:
                print("Error: Invalid number format in range. Please use integers.")
        else:
            print("Error: Invalid range format. Please use 'START-END' (e.g., 1-100).")


if __name__ == "__main__":
    # Determine the log file path in the script's directory
    script_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
    log_file_path = os.path.join(script_dir, "log.txt")

    with Logger(log_file_path):
        # Get ID range from user and update global variables
        START_ID, END_ID = _get_id_range_from_user()

        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\nScraping interrupted by user. Exiting gracefully.")
        except Exception as e:
            print(f"\nAn unexpected error occurred in main execution: {e}", file=sys.stderr)

