# CASE STUDY - Movie Data Pipeline

This repository implements a movie data ETL (Extract, Transform, Load) process that uses IMDb database, OMDB API, and Google Trends—to gather movie data. The goal is to build and maintain a cumulative master dataset of movies for further analysis and business insights.

## Repository Structure

```
/
├── README.md
├── presentation.pdf               # Slides running through implementation and use cases of pipeline.
├── data
│   ├── raw
│   │   ├── movies.xlsx            # Movies provided in case study
│   │   └── movies_gather.xlsx     # New movie titles generated by movie_gather.py
│   │   └── title.basics.tsv.gz    # **NOT INCLUDED DUE TO SIZE** IMDb database of movies.
│   └── processed
│       ├── movies_master.xlsx     # Cumulative master dataset appended by operationalise.py
│       └── movies_analysis.csv    # Cleaned movie data produced by analysis.py
└── src
    ├── analysis.py                # Enriches movie data using the OMDB API and Google Trends
    ├── operationalise.py          # Updates and appends new movie data to the master file
    └── movie_gather.py            # Uses IMDb database to gather new movie titles

```

## Overview

- **analysis.py:**  
  Reads the provided movie list from "movies.xlsx", gathers each entry with details from the OMDB API and normalised Google Trends search data using pytrends, and saves the resulting dataset as "movies_analysis.csv" in the "data/processed" folder.

- **movie_gather.py:**  
  Uses pandas to read IMDb database downloaded as "title.basics.tsv.gz", to gather 295,822 movies, movies must be within timeframe of 2006 < release < (current year).

- **operationalise.py:**  
  Appends movie data generated in movie_gather.py to "movies_master.xlsx" (stored in the "data/processed" folder), building a cumulative dataset over time, for more comprehensive modelling.

## Notes

- API key not included.
- pytrends API is very unstable, cookie catching is used to mitigate some of the problems. Google often blocks requests with error 429, as thinks user is a bot.
- In pytrends google interest search, interest is relative to film therefore "Feature film" is used as anchor term to compare movie interest with, in order to be able to compare movie interest.
- movies_master.xlsx is not appended with all form movies_gather.xlsx due to instability of pytrend.

  



