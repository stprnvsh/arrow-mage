"""
Test script for running a Fused UDF in local mode.
This shows how to create, run, and use UDFs without requiring a full Fused environment.
"""

import fused
import pandas as pd
from datetime import datetime

# Define a simple UDF that fetches top Hacker News stories
@fused.udf
def hacker_news_udf(story_type: str = "top", limit: int = 5):
    """
    Fetches top posts from Hacker News as a dataframe.

    Parameters:
    story_type (str): Type of stories to fetch. Options are:
                     - "top" for top stories
                     - "newest" for latest stories
    limit (int): Number of stories to fetch
    
    Returns:
    pandas.DataFrame: DataFrame containing HN posts
    """
    import pandas as pd
    import requests
    import time
    from datetime import datetime

    # Validate input
    if story_type not in ["top", "newest"]:
        raise ValueError('Invalid story_type. Must be "top" or "newest"')

    # Map story_type to the appropriate HN API endpoint
    endpoint_map = {"top": "topstories", "newest": "newstories"}
    endpoint = endpoint_map[story_type]

    # Fetch the list of story IDs
    response = requests.get(f"https://hacker-news.firebaseio.com/v0/{endpoint}.json")
    story_ids = response.json()

    # Limit the number of stories
    story_ids = story_ids[:limit]

    # Fetch details for each story ID
    stories = []
    for story_id in story_ids:
        try:
            # Get the story details
            story_response = requests.get(
                f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
            )
            story = story_response.json()

            # Skip if not a story or missing key fields
            if not story or story.get("type") != "story" or "title" not in story:
                continue

            # Add to our list
            stories.append(
                {
                    "id": story.get("id"),
                    "title": story.get("title"),
                    "url": story.get("url", ""),
                    "score": story.get("score", 0),
                    "by": story.get("by", ""),
                    "time": datetime.fromtimestamp(story.get("time", 0)),
                    "descendants": story.get("descendants", 0),
                }
            )

            # Brief pause to avoid overloading the API
            time.sleep(0.1)

        except Exception as e:
            print(f"Error fetching story {story_id}: {e}")

    # Convert the list of stories to a DataFrame
    df = pd.DataFrame(stories)

    # Add a timestamp for when the data was fetched
    df["fetched_at"] = datetime.now()
    
    return df

# --- Example 1: Run a UDF directly ---
print("\n--- Example 1: Running the UDF directly ---")
print("Getting top 3 Hacker News stories...\n")

# Run the UDF with the local engine to avoid environment errors
result = fused.run(hacker_news_udf, limit=3, engine='local')
print(f"Got {len(result)} stories")
print(result[['title', 'score', 'by']])

# --- Example 2: Load a UDF from the Fused community repository ---
print("\n--- Example 2: Loading a UDF from GitHub ---")
try:
    # Try loading a UDF from the Fused community repository
    # This may fail if network or GitHub access is restricted
    print("Loading DuckDB NYC Example UDF from GitHub...")
    github_udf = fused.load("https://github.com/fusedio/udfs/tree/main/public/DuckDB_NYC_Example")
    print("UDF loaded successfully")
    
    # Try running it (may require additional dependencies)
    try:
        print("Running the loaded UDF...")
        nyc_data = fused.run(udf=github_udf, engine='local')
        print(f"UDF ran successfully, returned data shape: {nyc_data.shape}")
    except Exception as e:
        print(f"Could not run the UDF: {e}")
except Exception as e:
    print(f"Could not load the UDF: {e}")

# --- Example 3: Save a UDF locally ---
print("\n--- Example 3: Saving a UDF locally ---")
try:
    print("Saving UDF to local directory...")
    hacker_news_udf.to_directory('hacker_news_udf_local')
    print("UDF saved successfully to 'hacker_news_udf_local' directory")
except Exception as e:
    print(f"Could not save the UDF: {e}")

print("\nTest complete!") 