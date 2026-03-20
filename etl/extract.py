import pandas as pd

path = "D:/covid-etl-pipeline/data/raw/imdb_tv_shows.csv"

#citire dataset
df = pd.read_csv(path)

#preview
print("==== Primele Randuri ===")
print(df.head())

print("\n=== Coloane ===")
print(df.columns)

print("\n=== Info ===")
print(df.info())

print("\n=== Dimensiune ===")
print(df.shape)

print("\n=== Null Values ===")
print(df.isnull().sum())

#verificare
duplicates = df.duplicated().sum()
print("\n=== Duplicate Rows ===")
print(duplicates)

#eliminare
df = df.drop_duplicates()
print("\n=== Dimensiune dupa remove duplicates ===")
print(df.shape)

#verificare tipuri date
print("\n=== Data Types ===")
print(df.dtypes)

#filling the nulls
df['averageRating'] = df['averageRating'].fillna(0.0)
df['numVotes'] = df['numVotes'].fillna(0)
df['startYear'] = df['startYear'].fillna(0)
df['endYear'] = df['endYear'].fillna(0)
df['genres'] = df['genres'].fillna("Unknown")
df['primaryTitle'] = df['primaryTitle'].fillna("Unknown")


#coloana tipul corect
df['averageRating'] = df['averageRating'].astype(float)
df['numVotes'] = df['numVotes'].astype(int)
df['startYear'] = df['startYear'].astype(int)
df['endYear'] = df['endYear'].astype(int)


#eliminare spatii inceput / sfarsit
df['primaryTitle'] = df['primaryTitle'].str.strip()
df['genres'] = df['genres'].str.strip()


#clean csv for TL
df.to_csv("../data/processed/imdb_tv_shows_clean.csv", index=False)
print("\n=== Data clean salvata in data/processed ===")

#rating
top_rating = df.sort_values(by='averageRating', ascending=False).head(10)
print("=== Top 10 show-uri dupa rating ===")
print(top_rating[['primaryTitle', 'averageRating', 'numVotes']])

#nr voturi
top_votes = df.sort_values(by='numVotes', ascending=False).head(10)
print("\n=== Top 10 show-uri dupa numVotes ===")
print(top_votes[['primaryTitle', 'averageRating', 'numVotes']])

#rating per gen

df_genres = df.assign(genres=df['genres'].str.split(',')) 
df_genres = df_genres.explode('genres') 

#strip spatii
df_genres['genres'] = df_genres['genres'].str.strip()

#agregare rating mediu per gen
rating_per_gen = df_genres.groupby('genres')['averageRating'].mean().sort_values(ascending=False)
print("\n=== Rating mediu per gen ===")
print(rating_per_gen.head(10))
