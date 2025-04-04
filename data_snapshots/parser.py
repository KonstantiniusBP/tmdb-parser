import os
import requests
import pandas as pd
import time
from dotenv import load_dotenv
import requests

load_dotenv()
TMDB_API_KEY = os.getenv("API_KEY")

BASE_URL = "https://api.themoviedb.org/3"
HEADERS = {"Authorization": f"Bearer {TMDB_API_KEY}"}
SNAPSHOT_FILE = "snapshot.txt"
OFFSET = 10000

def tmdb_request(endpoint, params=None):
    url = f"{BASE_URL}{endpoint}"
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        return None
    else:
        print(f"Error {response.status_code} for {url}")
        return None

def get_movie_details(movie_id):
    details = tmdb_request(f"/movie/{movie_id}", params={
        "append_to_response": "credits,keywords,images,videos,alternative_titles,external_ids,recommendations,similar"
    })

    if not details:
        return None

    return {
        "tmdb_id": movie_id,
        "imdb_id": details.get("imdb_id"),


        "original_title": details.get("original_title"),
        "description": details.get("overview"),
        "tmdb_rating": details.get("vote_average"),
        "tmdb_votes_count": details.get("vote_count"),
        "world_premiere": details.get("release_date"),
        "status": details.get("status"),
        "type": "Movie",
        "tagline": details.get("tagline"),


        "poster": f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else None,
        "background_image": f"https://image.tmdb.org/t/p/original{details.get('backdrop_path')}" if details.get("backdrop_path") else None,
        "movie_logo": f"https://image.tmdb.org/t/p/original{details.get('images', {}).get('logos', [{}])[0].get('file_path', '')}" if details.get('images', {}).get('logos') else None,
        "youtube_trailer": next((f"https://www.youtube.com/watch?v={vid['key']}" for vid in details.get("videos", {}).get("results", [])if vid["type"] == "Trailer" and vid["site"] == "YouTube" and vid.get("official") == True), None) or next((f"https://www.youtube.com/watch?v={vid['key']}" for vid in details.get("videos", {}).get("results", []) if vid["type"] == "Trailer" and vid["site"] == "YouTube"), None),


        "director": ", ".join([crew["name"] for crew in details.get("credits", {}).get("crew", []) if crew["job"] == "Director"]),
        "actors": ", ".join([f"{actor['name']} ({actor['character']})" if actor.get('character') else actor['name'] for actor in details.get("credits", {}).get("cast", [])]),
        "actor_images": ", ".join([f"https://image.tmdb.org/t/p/w500{actor['profile_path']}" for actor in details.get("credits", {}).get("cast", []) if actor.get("profile_path")]),
        "crew": ", ".join([f"{crew['name']} ({crew['job']})" for crew in details.get("credits", {}).get("crew", [])]),
        "crew_images": ", ".join([f"https://image.tmdb.org/t/p/w500{crew['profile_path']}" for crew in details.get("credits", {}).get("crew", []) if crew.get('profile_path')]),

        "runtime": details.get("runtime"),
        "budget": details.get("budget"),
        "revenue": details.get("revenue"),

        "genres": ", ".join([genre["name"] for genre in details.get("genres", [])]),
        "tags": ", ".join([tag["name"] for tag in details.get("keywords", {}).get("keywords", [])]),
        "original_language": details.get("original_language"),
        "spoken_languages": ", ".join([lang["english_name"] for lang in details.get("spoken_languages", [])]),
        "studios": ", ".join([company["name"] for company in details.get("production_companies", [])]),
        "country": ", ".join([country["name"] for country in details.get("production_countries", [])]),
        "link_to_tmdb_page": f"https://www.themoviedb.org/movie/{movie_id}",

        "collection_name": details.get("belongs_to_collection", {}).get("name") if details.get("belongs_to_collection") else None,
        "collection_poster": f"https://image.tmdb.org/t/p/w500{details['belongs_to_collection']['poster_path']}" if details.get("belongs_to_collection") and details["belongs_to_collection"].get("poster_path") else None,
        "collection_backdrop": f"https://image.tmdb.org/t/p/w500{details['belongs_to_collection']['backdrop_path']}" if details.get("belongs_to_collection") and details["belongs_to_collection"].get("backdrop_path") else None,
        "alternative_titles": ", ".join([alt["title"] for alt in details.get("alternative_titles", {}).get("titles", [])]),

        "facebook_id": details.get("external_ids", {}).get("facebook_id"),
        "instagram_id": details.get("external_ids", {}).get("instagram_id"),
        "twitter_id": details.get("external_ids", {}).get("twitter_id"),
        "site": details.get("homepage"),

        "recommended_movie_id": ", ".join([str(movie["id"]) for movie in details.get("recommendations", {}).get("results", [])]),
        "similar_movie_id": ", ".join([str(movie["id"]) for movie in details.get("similar", {}).get("results", [])]),

        "is_adult": details.get("adult"),
    }

def read_snapshot():
    if os.path.exists(SNAPSHOT_FILE):
        with open(SNAPSHOT_FILE, "r") as f:
            return int(f.read().strip())
    return 1

def write_snapshot(current_id):
    with open(SNAPSHOT_FILE, "w") as f:
        f.write(str(current_id))

def save_to_csv(data, iteration):
    df = pd.DataFrame(data)
    df.to_csv(f"data-{iteration}.csv", index=False, encoding="utf-8")
    print(f"Saved data-{iteration}.csv")

def main():
    current_id = read_snapshot()
    all_movies = []
    max_id = 1000000  

    delay = 0.02 
    max_delay = 5 

    print(f"Starting from TMDB ID {current_id} up to {max_id}...\n")

    for movie_id in range(current_id, max_id):
        print(f"Parsing movie ID {movie_id}", end="\r")

        try:
            movie_data = get_movie_details(movie_id)
            if movie_data:
                all_movies.append(movie_data)
                print(f"Parsed: {movie_id} ({len(all_movies)} movies in current batch)", end="\r")

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print(f"\nRate limit hit at ID {movie_id}. Slowing down...")
                delay = min(delay * 2, max_delay)
                time.sleep(delay)
                continue
            else:
                print(f"\nHTTP error at ID {movie_id}: {e}")
        except Exception as e:
            print(f"\nError at ID {movie_id}: {e}")

        if movie_id % 50 == 0:
            print(f"\nChecked up to ID {movie_id} (delay: {delay:.2f}s)")


        if len(all_movies) >= 10000:
            iteration = movie_id // OFFSET
            save_to_csv(all_movies, iteration)
            write_snapshot(movie_id)
            print(f"\nSnapshot saved at ID {movie_id}")
            all_movies = []

        time.sleep(delay)

    if all_movies:
        save_to_csv(all_movies, "final")
        write_snapshot(movie_id)
        print(f"\nFinal save complete at ID {movie_id}")


if __name__ == "__main__":
    main()
