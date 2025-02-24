import logging
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import requests
from pytrends.exceptions import ResponseError
from pytrends.request import TrendReq
from requests.exceptions import RetryError
from selenium import webdriver


current_dir = Path(__file__).parent

API_KEY = ""
OMDB_BASE_URL = "http://www.omdbapi.com/"
EXCEL_FILE = (current_dir / '..' / 'data' / 'raw' / 'movies_gather.xlsx').resolve()
MASTER_EXCEL_FILE = (current_dir / '..' / 'data' / 'processed' / 'movies_master.xlsx').resolve()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_cookie() -> str:
    """
    Retrieves the 'NID' cookie value from Google Trends.
    """
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    driver = webdriver.Chrome(options=options)
    driver.get("https://trends.google.com/")
    time.sleep(5)
    cookie = driver.get_cookie("NID")
    driver.quit()
    if cookie:
        return cookie["value"]
    logging.error("Failed to retrieve NID cookie")
    return ""

def get_searches(pytrends: TrendReq, title: str, timeframe: str, attempts: int = 5) -> float:
    """
    Retrieves the Google search interest for a given title within a timeframe.
    Pytrends only gives relative search interest, therefore normalisation must
    be done by comparing the movie's search interest to the 'Feature film' anchor.
    Retries on certain exceptions.
    """
    for attempt in range(attempts):
        try:
            #build payload with both the movie term and the anchor keyword "Feature film"
            pytrends.build_payload(kw_list=[title, "Feature film"], cat=0, timeframe=timeframe, geo='', gprop='')
            interest_data = pytrends.interest_over_time()

            #calculate mean search interest for the movie term and the anchor term
            movie_interest = interest_data[title].mean()
            anchor_interest = interest_data["Feature film"].mean()

            #avoid division by zero; if the anchor is 0, return 0
            if anchor_interest == 0:
                return 0.0

            #return the normalised interest value
            normalised_interest = movie_interest / anchor_interest
            return normalised_interest

        except (ResponseError, RetryError, IndexError) as e:
            logging.warning(f"Attempt {attempt + 1}/{attempts} failed for '{title}': {e}")
            time.sleep(2.5)
    return 0.0

def fetch_movie_data(title: str, session: requests.Session) -> dict:
    """
    Fetches movie data from the OMDB API.
    """
    params = {"t": title, "apikey": API_KEY}
    response = session.get(OMDB_BASE_URL, params=params)
    return response.json()

def process_movie_data(data: dict) -> dict:
    """
    Processes the raw movie data and returns a cleaned dictionary of values.
    Returns an empty dictionary if the movie doesn't meet the criteria.
    """
    try:
        release_year = int(data.get("Year", 0))
    except (ValueError, TypeError):
        return {}

    if release_year <= 2006:
        return {}

    release_str = data.get("Released", "N/A")
    if release_str == "N/A":
        return {}

    try:
        release_date = datetime.strptime(release_str, "%d %b %Y")
    except ValueError:
        return {}

    box_office_str = data.get("BoxOffice", "N/A")
    if box_office_str != "N/A":
        box_office = box_office_str.replace("$", "").replace(",", "")      #removing commas from box office to return as integer
        try:
            box_office = int(box_office)
        except ValueError:
            return {}
    else:
        return {}

    imdb_votes_str = data.get("imdbVotes", "N/A")
    imdb_votes = int(imdb_votes_str.replace(",", "")) if imdb_votes_str != "N/A" else 0  #similar remove commas

    runtime_str = data.get("Runtime", "N/A")
    if runtime_str != "N/A" and "min" in runtime_str:
        try:
            runtime = int(runtime_str.replace(" min", "").strip()) 
        except ValueError:
            runtime = 0
    else:
        runtime = 0

    return {
        "Title": data.get("Title", "N/A"),
        "Year": data.get("Year", "N/A"),
        "Runtime (mins)": runtime,
        "IMDb Rating": data.get("imdbRating", "N/A"),
        "IMDb Votes": imdb_votes,
        "Box Office ($)": box_office,
        "Release Date": release_date,
        "Age Rating": data.get("Rated", "N/A")
    }

def main():
    nid_cookie = get_cookie()
    if not nid_cookie:
        logging.error("Exiting due to missing cookie.")
        return

    requests_args = {
        'headers': {
            'Cookie': f"NID={nid_cookie}",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36'
        }
    }
    pytrends = TrendReq(retries=3, requests_args=requests_args)

    movies_df = pd.read_excel(EXCEL_FILE)
    movie_titles = movies_df['title'].tolist()

    processed_movies = []
    session = requests.Session()

    try:
        existing_df = pd.read_excel(MASTER_EXCEL_FILE)
        existing_movies = set(existing_df.iloc[:, 0].astype(str).str.strip())
    except Exception:
        existing_movies = set()

    for title in movie_titles:
        logging.info(f"Processing movie: {title}")

        if title in existing_movies:
            logging.info(f"Skipping movie '{title}' due to already being in master.")
            continue

        data = fetch_movie_data(title, session)
        movie = process_movie_data(data)

        if not movie:
            logging.info(f"Skipping movie '{title}' due to incomplete or invalid data.")
            continue

        search_term = f"{title} Movie"
        
        release_date = movie.pop("Release Date")

        ##In order to get years worth of google interest data, we must check that movie was not released less than a year ago.
        #since the script runs on the first day of the month, use today's date as the start of the month.
        current_month = datetime.now().replace(day=1)

        #calculate the cutoff date: one year before the start of the current month.
        cutoff_date = current_month - timedelta(days=365)

        #skip the movie if its release date is later than the cutoff.
        #(this means we only process movies released on or before the cutoff date.)
        if release_date > cutoff_date:
            logging.info(
                f"Skipping movie '{title}' because it was released on {release_date.strftime('%Y-%m-%d')}, which is after the cutoff date of {cutoff_date.strftime('%Y-%m-%d')}."
            )
            continue

        end_date = release_date + timedelta(days=365)
        timeframe = f"{release_date.strftime('%Y-%m-%d')} {end_date.strftime('%Y-%m-%d')}"
        searches = get_searches(pytrends, search_term, timeframe)

        if searches == 0:
            logging.info(f"No search interest data for '{search_term}'. Skipping.")
            continue

        movie["Google Interest"] = searches
        processed_movies.append(movie)
        logging.info(f"Appended data for '{search_term}'.")

    if processed_movies:
        new_data_df = pd.DataFrame(processed_movies)
        try:
            existing_df = pd.read_excel(MASTER_EXCEL_FILE)
            combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
            logging.info("Appended new data to existing master file.")
        except Exception as e:
            logging.info(f"No existing master file found or error reading file: {e}. Creating new master file.")
            combined_df = new_data_df

        combined_df.to_excel(MASTER_EXCEL_FILE, index=False)
        logging.info(f"Excel file saved to {MASTER_EXCEL_FILE}.")
    else:
        logging.info("No movies were processed successfully.")

main()
