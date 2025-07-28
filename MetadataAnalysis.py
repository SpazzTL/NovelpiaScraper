import json
import time
from collections import Counter
import os

def calculate_average_chapters(jsonl_filepath, selected_tags=None, tag_filter_mode='any', only_completed=False, populate_tags_only=False):
    """
    Calculates the average 'chapter_count' from a .jsonl file,
    with optional filtering by multiple tags (inclusive/exclusive) and publication status.

    Args:
        jsonl_filepath (str): The path to the .jsonl file.
        selected_tags (list, optional): A list of tags.
        tag_filter_mode (str): 'any' (inclusive - record must have at least one selected tag)
                                or 'all' (exclusive - record must have ALL selected tags). Defaults to 'any'.
        only_completed (bool, optional): Only include records with "publication_status": "완결". Defaults to False.
        populate_tags_only (bool, optional): If True, only populate the all_tags counter and skip chapter counting.

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

                    # Apply tag filters (if any selected)
                    if selected_tags:
                        record_tags = record.get('tags', [])
                        if not isinstance(record_tags, list): # Ensure tags is a list
                            record_tags = []

                        tag_match = False
                        if tag_filter_mode == 'any': # Inclusive (OR logic)
                            tag_match = any(tag in record_tags for tag in selected_tags)
                        elif tag_filter_mode == 'all': # Exclusive (AND logic)
                            tag_match = all(tag in record_tags for tag in selected_tags)
                        
                        if not tag_match:
                            continue # Skip if the tag filter condition is not met

                    # Apply completion status filter
                    if only_completed:
                        if 'publication_status' not in record or record['publication_status'] != '완결':
                            continue # Skip if only completed works are required and not met

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
                    # print(f"Skipping malformed JSON line: {line.strip()}") # Optional: uncomment for debugging
                    pass
                except KeyError as e:
                    # print(f"Skipping line due to missing key: {e} in {line.strip()}") # Optional: uncomment for debugging
                    pass
                except Exception as e:
                    # print(f"An unexpected error occurred processing line: {line.strip()} - {e}") # Optional: uncomment for debugging
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
    Saves the ID and title of filtered records to a text file.

    Args:
        records_info (list): A list of dictionaries, each containing 'id' and 'title'.
        filename (str): The name of the file to save the results to.
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            for record in records_info:
                f.write(f"ID: {record.get('id', 'N/A')}, Title: {record.get('title', 'N/A')}\n")
        print(f"\nFiltered results saved to '{filename}'.")
    except Exception as e:
        print(f"Error saving results to file '{filename}': {e}")

if __name__ == "__main__":
    file_path = input("Please enter the full path to your .jsonl file: ").strip()

    # --- First Pass: Analyze tags and get top 30 ---
    print("\nAnalyzing tags to find popular ones (this might take a moment for large files)...")
    _, all_tags_counter, _ = calculate_average_chapters(file_path, populate_tags_only=True)

    popular_tags = all_tags_counter.most_common(30)
    
    # Create a mapping for numeric selection
    popular_tag_map = {}
    if popular_tags:
        print("\n--- Top 30 Most Popular Tags ---")
        for i, (tag, count) in enumerate(popular_tags):
            print(f"{i+1}. {tag} ({count} occurrences)")
            popular_tag_map[str(i+1)] = tag # Store number as string key
        print("---------------------------------")
    else:
        print("No tags found in the file or file could not be read. Cannot suggest tags.")

    # --- Get Filter Choices from the User ---
    chosen_tags = []
    tag_filter_mode = 'any' # Default to inclusive (OR)
    
    if popular_tags:
        print("\nEnter tags to filter by. You can:")
        print("  - Enter numbers (e.g., 1, 5, 10) to select from the list above.")
        print("  - Enter any tag text (e.g., #판타지, #코미디).")
        print("  - Mix numbers and text (e.g., 1, #현대, 5).")
        print("  - Press Enter without typing anything to include all tags.")
        tags_input = input("Your selected tags (comma-separated): ").strip()
        
        if tags_input:
            input_items = [item.strip() for item in tags_input.split(',') if item.strip()]
            
            for item in input_items:
                if item.isdigit() and item in popular_tag_map:
                    chosen_tags.append(popular_tag_map[item])
                else:
                    chosen_tags.append(item)
            
            # Remove duplicates while preserving order (optional, but good practice)
            chosen_tags = list(dict.fromkeys(chosen_tags)) 
            
            if chosen_tags:
                print(f"You selected these tags for filtering: {chosen_tags}")
                if len(chosen_tags) > 1: # Only ask for mode if multiple tags are selected
                    mode_choice = input("Filter mode for multiple tags: 'any' (novel has AT LEAST ONE of these tags) or 'all' (novel has ALL of these tags)? (default: any): ").strip().lower()
                    if mode_choice == 'all':
                        tag_filter_mode = 'all'
                    print(f"Using tag filter mode: '{tag_filter_mode}'")
            else:
                print("No valid tags were selected. All tags will be included.")

    completion_choice = input("Only include '완결' (completed) works? (yes/no, default no): ").strip().lower()
    only_completed_filter = (completion_choice == 'yes' or completion_choice == 'y')

    # --- Second Pass: Calculate Average with Filters and get filtered records ---
    print("\n--- Calculating Average Chapters with Filters ---")
    final_average, _, filtered_results_for_output = calculate_average_chapters(file_path, chosen_tags, tag_filter_mode, only_completed_filter, populate_tags_only=False)

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