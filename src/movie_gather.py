import pandas as pd
import time
from pathlib import Path

current_dir = Path(__file__).parent
output_file = (current_dir / '..' / 'data' / 'raw' / 'movies_gather.xlsx').resolve()
database_file = (current_dir / '..' / 'data' / 'raw' / 'title.basics.tsv.gz').resolve()  # gzipped TSV file

chunk_size = 10000  # increase chunk size if memory allows
film_list = []      # list to store filtered film titles

#only read the columns needed for memory efficiency
usecols = ['titleType', 'primaryTitle', 'startYear']

chunks = pd.read_csv(
    database_file, 
    sep='\t', 
    compression='gzip', 
    chunksize=chunk_size, 
    usecols=usecols, 
    low_memory=False,
    dtype={'titleType': 'category', 'primaryTitle': 'string', 'startYear': 'string'}  #read startYear as string for conversion later
)

year_int = time.localtime().tm_year

for chunk in chunks:
    #convert startYear to numeric, to n/a if not to not raise error.
    chunk['startYear'] = pd.to_numeric(chunk['startYear'], errors='coerce')
    
    #filter for movies with startYear > 2006 and also less than last_year_int
    movies = chunk[(chunk['titleType'] == 'movie') & (chunk['startYear'] > 2006) & (chunk['startYear'] < year_int)]
    
    #extract only the film title and rename the column to 'title'
    titles = movies['primaryTitle'].rename("title")
    
    if not titles.empty:
        film_list.append(titles)

final_titles = pd.concat(film_list, ignore_index=True).to_frame()
final_titles.to_excel(output_file, index=False)
print(f"Processed {len(final_titles)} movies.")
