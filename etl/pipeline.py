import pandas as pd
import os
import sqlite3

#etl
def extract(path="D:/covid-etl-pipeline/data/raw/imdb_tv_shows.csv"):
    df = pd.read_csv(path)
    return df

def transform(df):
    #drop duplicates
    df = df.drop_duplicates()

    #filling nulls
    df['averageRating'] = df['averageRating'].fillna(0.0)
    df['numVotes'] = df['numVotes'].fillna(0)
    df['startYear'] = df['startYear'].fillna(0)
    df['endYear'] = df['endYear'].fillna(0)
    df['genres'] = df['genres'].fillna("Unknown")
    df['primaryTitle'] = df['primaryTitle'].fillna("Unknown")

    #casting
    df['averageRating'] = df['averageRating'].fillna(0.0)
    df['numVotes'] = df['numVotes'].fillna(0)
    df['startYear'] = df['startYear'].fillna(0)
    df['endYear'] = df['endYear'].fillna(0)
    df['genres'] = df['genres'].fillna("Unknown")
    df['primaryTitle'] = df['primaryTitle'].fillna("Unknown")

    #clean
    df['primaryTitle'] = df['primaryTitle'].str.strip()
    df['genres'] = df['genres'].str.strip()

    return df

def load(df):
    #afisare si returnare date load in db
    #top 10 shows dupa rating
    top_rating = df.sort_values(by='averageRating', ascending=False).head(10)
    print("=== Top 10 show-uri dupa rating ===")
    print(top_rating[['primaryTitle', 'averageRating', 'numVotes']])

    #top 10 shows dupa voturi
    top_votes = df.sort_values(by='numVotes', ascending=False).head(10)
    print("\n=== Top 10 show-uri dupa numVotes ===")
    print(top_votes[['primaryTitle', 'averageRating', 'numVotes']])

    #rating mediu per gen
    df_genres = df.assign(genres=df['genres'].str.split(','))
    df_genres = df_genres.explode('genres')
    df_genres['genres'] = df_genres['genres'].str.strip()
    rating_per_gen = df_genres.groupby('genres')['averageRating'].mean().sort_values(ascending=False)

    print("\n=== Rating mediu per gen ===")
    print(rating_per_gen.head(10))
    return top_rating, top_votes, rating_per_gen

def load_to_db(df, rating_per_gen):
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_path = os.path.join(BASE_DIR, "data", "imdb_tv_shows.db")

    #connect baza de date
    conn = sqlite3.connect(db_path)

    #salvez shows curatate
    df.to_sql("shows", conn, if_exists="replace", index=False)

    #salvez rating mediu per gen
    rating_per_gen = rating_per_gen.reset_index()
    rating_per_gen.columns = ['genre', 'averageRating']
    rating_per_gen.to_sql("genres_rating", conn, if_exists="replace", index=False)

    conn.close()
    print(f"=== Datele au fost incarcate in db")

if __name__ == "__main__":
        df = extract()
        df = transform(df)
        top_rating, top_votes, rating_per_gen = load(df)
        load_to_db(df, rating_per_gen)