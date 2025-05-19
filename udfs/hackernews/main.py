import pandas as pd
import requests

def main(story_type: str = "top"):
    """
    Fetch Hacker News stories based on the specified story type.
    
    Args:
        story_type: Type of stories to fetch (top, new, best, ask, show, job)
    
    Returns:
        DataFrame containing the story data
    """
    # Map story types to API endpoints
    story_type_endpoints = {
        "top": "topstories",
        "new": "newstories",
        "best": "beststories",
        "ask": "askstories",
        "show": "showstories",
        "job": "jobstories",
        "newest": "newstories"  # Alias for "new"
    }
    
    # Use the appropriate endpoint or default to topstories
    endpoint = story_type_endpoints.get(story_type.lower(), "topstories")
    
    # Fetch story IDs
    story_ids_url = f"https://hacker-news.firebaseio.com/v0/{endpoint}.json"
    story_ids = requests.get(story_ids_url).json()
    
    # Limit to top 10 stories for performance
    story_ids = story_ids[:10]
    
    # Fetch details for each story
    stories = []
    for story_id in story_ids:
        story_url = f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
        story = requests.get(story_url).json()
        if story:
            stories.append(story)
    
    # Convert to DataFrame
    df = pd.DataFrame(stories)
    
    # Select and rename relevant columns if they exist
    columns_to_keep = ["id", "title", "url", "score", "by", "time", "descendants"]
    df = df[[col for col in columns_to_keep if col in df.columns]]
    
    return {"data": df.to_dict(orient="records")}
