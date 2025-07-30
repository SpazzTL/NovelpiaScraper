# NovelpiaScraper

(Very inefficient, brute forces everything. I may make a better method sometime) <br> <br> 
Scrapes novelpia to fetch novel metadata (title, synopsis, author, tags, age, status) to JSONL.
 <br> <br> 
Along with some other .py files to organize and handle the data
## How to improve this shitty script
When you search on Novelpia, you get a URL like this:
https://novelpia.com/search/all//1/판타지?page=1&rows=100&novel_type=&start_count_book=&end_count_book=&novel_age=&start_days=&sort_col=count_view&novel_genre=&block_out=0&block_stop=0&is_contest=0&is_complete=&is_challenge=0&list_display=list
<br>
<br>


    %ED%8C%90%ED%83%80%EC%A7%80 is just 판타지 (the tag) in a different format. 

The key part is everything after the question mark. 

    rows=100 . The default is 30~, but you can change it up to 100-1000 to get more results per page || request.

    Once you see how many total novels there are (e.g., "총 35,766개 작품"), you can just have your script loop through all the pages. For this example, that's pages 1 to 357 (and maybe 358 just to be safe).

This lets you grab a ton of metadata (author, views, etc.) from every single novel without making a unique request for each one, avoiding ip bans and data usage. The only issue is you won't get the synopsis this way, so you then use this list of valid ids/etc to THEN scrape like I do here (go through each id and load /novel/id html content and scrape what you need. [Avoids making 380,000 request and indexing deleted novels.. Though you need an adult verified account to search and index adult content this way] )

Then, you can:

    Grab all the tags from your first big scrape.

    Have your script automatically search for each tag.

    Make sure it skips any tags it's already searched for so you don't waste time getting the same novels twice.

  # images
<img width="883" height="416" alt="image" src="https://github.com/user-attachments/assets/38295363-1273-4f94-a848-5c045cbe730a" />
<img width="1254" height="1198" alt="image" src="https://github.com/user-attachments/assets/4e309185-e3ab-430a-99b3-916a631cc334" />
