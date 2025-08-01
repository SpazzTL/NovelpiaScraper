import json
import time
from collections import Counter
import os

def calculate_average_chapters(jsonl_filepath, required_tags=None, optional_tags=None, only_completed=False, populate_tags_only=False, min_likes=0, min_chapters=0, include_adult='all'):
    """
    Calculates the average 'chapter_count' from a .jsonl file,
    with optional filtering by required tags, optional tags, publication status,
    minimum likes, minimum chapters, and adult content.

    Args:
        jsonl_filepath (str): The path to the .jsonl file.
        required_tags (list, optional): A list of tags that ALL must be present in a record. Defaults to None.
        optional_tags (list, optional): A list of tags where AT LEAST ONE must be present in a record. Defaults to None.
        only_completed (bool, optional): Only include records with "publication_status": "완결". Defaults to False.
        populate_tags_only (bool, optional): If True, only populate the all_tags counter and skip chapter counting.
        min_likes (int, optional): Minimum 'total_likes' a record must have. Defaults to 0.
        min_chapters (int, optional): Minimum 'chapter_count' a record must have. Defaults to 0.
        include_adult (str, optional): 'yes' to include adult, 'no' to exclude, 'all' to ignore filter. Defaults to 'all'.

    Returns:
        tuple: (average_chapters, all_tags_counter, filtered_records_info)
               average_chapters (float): The average chapter count, or 0 if no data is processed.
               all_tags_counter (Counter): A Counter object with all tags and their frequencies.
               filtered_records_info (list): A list of dictionaries, each containing 'id' and 'title'
                                             for records that passed all filters. Empty if populate_tags_only is True.
    """
    total_chapters = 0
    record_count = 0
    all_tags = Counter() # To count tag occurrences
    filtered_records_info = [] # To store id and title of filtered records

    start_time = time.time()

    try:
        with open(jsonl_filepath, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)

                    # Accumulate all tags for popularity calculation (always do this on every pass)
                    if 'tags' in record and isinstance(record['tags'], list):
                        for tag in record['tags']:
                            all_tags[tag] += 1

                    if populate_tags_only:
                        continue # If only populating tags, skip the rest of the filtering/counting

                    record_tags = record.get('tags', [])
                    if not isinstance(record_tags, list): # Ensure tags is a list
                        record_tags = []

                    # Apply REQUIRED tags filter (AND logic)
                    if required_tags:
                        if not all(tag in record_tags for tag in required_tags):
                            continue # Skip if any required tag is missing

                    # Apply OPTIONAL tags filter (OR logic)
                    if optional_tags:
                        if not any(tag in record_tags for tag in optional_tags):
                            continue # Skip if none of the optional tags are present

                    # Apply completion status filter
                    if only_completed:
                        if 'publication_status' not in record or record['publication_status'] != '완결':
                            continue # Skip if only completed works are required and not met

                    # Apply minimum likes filter
                    # Check if 'total_likes' key exists and is a number before accessing it
                    if 'total_likes' in record and isinstance(record['total_likes'], (int, float)):
                        if record['total_likes'] < min_likes:
                            continue
                    elif min_likes > 0: # If min_likes is set but 'total_likes' is missing, skip
                        continue

                    # Apply minimum chapters filter
                    if 'chapter_count' in record and isinstance(record['chapter_count'], (int, float)):
                        if record['chapter_count'] < min_chapters:
                            continue
                    elif min_chapters > 0: # If min_chapters is set but 'chapter_count' is missing, skip
                        continue

                    # Apply adult content filter
                    # Assuming 'is_adult' is a boolean field. Default to False if missing.
                    if include_adult == 'no':
                        if record.get('is_adult', False):
                            continue
                    elif include_adult == 'yes':
                        if not record.get('is_adult', False):
                            continue

                    # If all filters pass, include in chapter count and store info
                    if 'chapter_count' in record and isinstance(record['chapter_count'], (int, float)):
                        total_chapters += record['chapter_count']
                        record_count += 1
                        # Store ID and Title for filtered results
                        if 'id' in record and 'title' in record:
                            filtered_records_info.append({
                                'id': record['id'],
                                'title': record['title']
                            })

                except json.JSONDecodeError:
                    pass
                except KeyError as e:
                    pass
                except Exception as e:
                    pass

    except FileNotFoundError:
        print(f"Error: File not found at '{jsonl_filepath}'. Please ensure the path is correct.")
        return 0, Counter(), [] # Return 0, empty tags, empty list if file not found
    except Exception as e:
        print(f"An error occurred while opening or reading the file: {e}")
        return 0, Counter(), []

    end_time = time.time()
    processing_time = end_time - start_time

    if populate_tags_only:
        return 0, all_tags, [] # Only return tags if that's what was requested for this pass

    # This part only runs if we're actually calculating averages
    if record_count > 0:
        average_chapters = total_chapters / record_count
        print(f"\n--- Processing Complete ---")
        print(f"Processed {record_count} records in {processing_time:.4f} seconds.")
        print(f"Total chapters summed: {total_chapters}")
        print(f"**Average chapter count for filtered records: {average_chapters:.2f}**")
        return average_chapters, all_tags, filtered_records_info
    else:
        print("No valid records found based on your criteria.")
        return 0, all_tags, []

def save_results_to_file(records_info, filename="results.txt"):
    """
    Saves the ID and title of filtered records to a text file in 'title, id' format.

    Args:
        records_info (list): A list of dictionaries, each containing 'id' and 'title'.
        filename (str): The name of the file to save the results to.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for record in records_info:
                # Changed order to title, id
                f.write(f"{record.get('title', 'N/A')}, {record.get('id', 'N/A')}\n") 
        print(f"\nFiltered results saved to '{filename}'.")
    except Exception as e:
        print(f"Error saving results to file '{filename}': {e}")

# Helper function to parse tag input from user
def parse_tag_input(tag_input_string, popular_tag_map):
    parsed_tags = []
    if tag_input_string:
        input_items = [item.strip() for item in tag_input_string.split(',') if item.strip()]
        for item in input_items:
            if item.isdigit() and item in popular_tag_map:
                parsed_tags.append(popular_tag_map[item])
            else:
                parsed_tags.append(item)
        parsed_tags = list(dict.fromkeys(parsed_tags)) # Remove duplicates
    return parsed_tags


if __name__ == "__main__":
    file_path = input("Please enter the full path to your .jsonl file: ").strip()

    # --- First Pass: Analyze tags and get top 50 ---
    print("\nAnalyzing tags to find popular ones (this might take a moment for large files)...")
    _, all_tags_counter, _ = calculate_average_chapters(file_path, populate_tags_only=True)

    popular_tags = all_tags_counter.most_common(50)
    
    # Create a mapping for numeric selection
    popular_tag_map = {}
    if popular_tags:
        print("\n--- Top 50 Most Popular Tags ---")
        for i, (tag, count) in enumerate(popular_tags):
            print(f"{i+1}. {tag} ({count} occurrences)")
            popular_tag_map[str(i+1)] = tag # Store number as string key
        print("---------------------------------")
    else:
        print("No tags found in the file or file could not be read. Cannot suggest tags.")

    # --- Get Filter Choices from the User ---
    
    print("\nEnter tags for filtering. You can:")
    print("  - Enter numbers (e.g., 1, 5, 10) to select from the list above.")
    print("  - Enter any tag text (e.g., #판타지, #코미디).")
    print("  - Mix numbers and text (e.g., 1, #현대, 5).")

    required_tags_input = input("\nEnter tags that *must* all be present (comma-separated, leave blank if none): ").strip()
    required_filters = parse_tag_input(required_tags_input, popular_tag_map)
    if required_filters:
        print(f"Required tags: {required_filters}")

    optional_tags_input = input("Enter tags where *at least one* must be present (comma-separated, leave blank if none): ").strip()
    optional_filters = parse_tag_input(optional_tags_input, popular_tag_map)
    if optional_filters:
        print(f"Optional tags (at least one of these): {optional_filters}")

    completion_choice = input("\nOnly include '완결' (completed) works? (yes/no, default no): ").strip().lower()
    only_completed_filter = (completion_choice == 'yes' or completion_choice == 'y')

    min_likes_input = input("Minimum number of likes (default 0): ").strip()
    min_likes_filter = int(min_likes_input) if min_likes_input.isdigit() else 0

    min_chapters_input = input("Minimum number of chapters (default 0): ").strip()
    min_chapters_filter = int(min_chapters_input) if min_chapters_input.isdigit() else 0

    adult_choice = input("Include adult content? ('yes', 'no', or 'all' to ignore filter, default 'all'): ").strip().lower()
    if adult_choice not in ['yes', 'no', 'all']: 
        adult_choice = 'all' # Default to 'all' if invalid input, meaning no adult filter applied

    # --- Second Pass: Calculate Average with Filters and get filtered records ---
    print("\n--- Calculating Average Chapters with Filters ---")
    final_average, _, filtered_results_for_output = calculate_average_chapters(
        file_path,
        required_tags=required_filters,
        optional_tags=optional_filters,
        only_completed=only_completed_filter,
        populate_tags_only=False,
        min_likes=min_likes_filter,
        min_chapters=min_chapters_filter,
        include_adult=adult_choice
    )

    if final_average > 0:
        print(f"\nFinal average chapter count for your selected criteria: {final_average:.2f}")

    # --- Ask to save results ---
    if filtered_results_for_output: # Only ask if there are results to save
        save_choice = input("\nDo you want to save the list of filtered novel IDs and titles to a file? (yes/no, default no): ").strip().lower()
        if save_choice == 'yes' or save_choice == 'y':
            output_filename = input("Enter filename (default: results.txt): ").strip()
            if not output_filename:
                output_filename = "results.txt"
            
            # Ensure the filename has a .txt extension if not provided
            if not output_filename.lower().endswith('.txt'):
                output_filename += '.txt'

            save_results_to_file(filtered_results_for_output, output_filename)
    else:
        print("\nNo records were filtered, so no output file will be generated.")

    # --- PAUSE SCRIPT ---
    input("\nPress Enter to exit...")