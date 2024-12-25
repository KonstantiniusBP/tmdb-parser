import os
import requests
import sys
import pandas as pd
from dotenv import load_dotenv

# Set console encoding to UTF-8
if os.name == 'nt':
    os.system('chcp 65001')
sys.stdout.reconfigure(encoding='utf-8')

# Load variables from .env
load_dotenv()
TMDB_API_KEY = os.getenv('API_KEY')

# Set up folder for saving files
OUTPUT_FOLDER = "tmdb_dataset"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Function to make requests to TMDB API
def tmdb_request(endpoint, params=None):
    base_url = "https://api.themoviedb.org/3"
    headers = {"Authorization": f"Bearer {TMDB_API_KEY}"}
    url = f"{base_url}{endpoint}"
    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()
    return response.json()

# Get Tarantino movies IDs
def get_tarantino_movies():
    tarantino_id = 138  # TMDB ID for Quentin Tarantino
    filmography = tmdb_request(f"/person/{tarantino_id}/movie_credits")
    movies = [
        movie for movie in filmography["crew"]
        if movie["job"] == "Director"
    ]
    return movies

# Parse movie details
def parse_movie_details(movie_id):
    details = tmdb_request(f"/movie/{movie_id}")
    images = tmdb_request(f"/movie/{movie_id}/images")
    return {
        "Title": details.get("title"),
        "Original Title": details.get("original_title"),
        "Overview": details.get("overview"),
        "Director": "Quentin Tarantino",
        "Actors": ", ".join([cast["name"] for cast in details.get("credits", {}).get("cast", [])[:5]]),
        "Genres": ", ".join([genre["name"] for genre in details.get("genres", [])]),
        "TMDB Page URL": f"https://www.themoviedb.org/movie/{movie_id}",
        "TMDB Rating": details.get("vote_average"),
        "Release Date": details.get("release_date"),
        "Budget": details.get("budget"),
        "Revenue": details.get("revenue"),
        "Runtime": details.get("runtime"),
        "Status": details.get("status"),
        "Tagline": details.get("tagline"),
        "Original Language": details.get("original_language"),
        "Production Companies": ", ".join([company["name"] for company in details.get("production_companies", [])]),
        "Production Countries": ", ".join([country["name"] for country in details.get("production_countries", [])]),
        "TMDB ID": movie_id,
        "IMDB ID": details.get("imdb_id"),
        "Keywords": ", ".join([tag["name"] for tag in details.get("keywords", {}).get("keywords", [])]),
        "Recommended Movie IDs": ", ".join([str(movie["id"]) for movie in details.get("recommendations", {}).get("results", [])]),
        "Similar Movie IDs": ", ".join([str(movie["id"]) for movie in details.get("similar", {}).get("results", [])]),
        "Poster": f"https://image.tmdb.org/t/p/w500{details.get('poster_path')}" if details.get("poster_path") else None,
        "Additional Posters": ", ".join([f"https://image.tmdb.org/t/p/w500{img['file_path']}" for img in images.get("posters", [])]),
        "Logos": ", ".join([f"https://image.tmdb.org/t/p/w500{img['file_path']}" for img in images.get("logos", [])]),
        "Backdrop": f"https://image.tmdb.org/t/p/w500{details.get('backdrop_path')}" if details.get("backdrop_path") else None,
        "Collection Name": details.get("belongs_to_collection", {}).get("name") if details.get("belongs_to_collection") else None,
        "Collection Poster": f"https://image.tmdb.org/t/p/w500{details.get('belongs_to_collection', {}).get('poster_path')}" if details.get("belongs_to_collection") else None,
        "Collection Backdrop": f"https://image.tmdb.org/t/p/w500{details.get('belongs_to_collection', {}).get('backdrop_path')}" if details.get("belongs_to_collection") else None,
        "Adult": "Yes" if details.get("adult") else "No",
    }

# Main process
def main():
    tarantino_movies = get_tarantino_movies()
    dataset = []

    for movie in tarantino_movies:
        movie_id = movie["id"]
        print(f"Parsing movie: {movie['title']}")
        try:
            movie_data = parse_movie_details(movie_id)
            dataset.append(movie_data)
        except Exception as e:
            print(f"Error processing {movie['title']}: {e}")

    # Save data to CSV
    df = pd.DataFrame(dataset)
    output_file = os.path.join(OUTPUT_FOLDER, "tarantino_movies.csv")
    counter = 1
    while os.path.exists(output_file):
        output_file = os.path.join(OUTPUT_FOLDER, f"tarantino_movies{counter}.csv")
        counter += 1
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"Data saved to {output_file}")

if __name__ == "__main__":
    main()
