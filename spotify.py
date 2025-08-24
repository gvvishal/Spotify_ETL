import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import mysql.connector

# ---------------- Spotify Auth -----------------
CLIENT_ID = "a3916f3632964dbe83e03a71218ef9af"
CLIENT_SECRET = "2345254c32404152a6d192d8ff6d5601"

auth_manager = SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
sp = spotipy.Spotify(auth_manager=auth_manager)

# ---------------- Fetch Artist ----------------
artist_id = "1mYsTxnqsietFxj1OgoGbG"  # Example: AR Rahman
artist = sp.artist(artist_id)
print(f"Artist: {artist['name']} | Followers: {artist['followers']['total']} | Genres: {artist['genres']}")

# ---------------- Fetch All Albums & Tracks ----------------
tracks_data = []

albums = sp.artist_albums(artist_id, album_type="album,single", limit=50)
album_ids = list({album['id'] for album in albums['items']})  # unique albums only

for album_id in album_ids:
    album = sp.album(album_id)
    album_name = album['name']
    release_date = album['release_date']

    tracks = sp.album_tracks(album_id)
    for track in tracks['items']:
        tracks_data.append({
            "track_name": track['name'],
            "album": album_name,
            "release_date": release_date,
            "popularity": sp.track(track['id'])['popularity'],  # extra API call
            "duration_ms": track['duration_ms'],
        })

print(f"✅ Total Tracks Fetched: {len(tracks_data)}")

# ---------------- Save to CSV ----------------
df = pd.DataFrame(tracks_data).drop_duplicates(subset=["track_name", "album"])
df.to_csv("spotify_all_tracks.csv", index=False, encoding="utf-8")
print("✅ Data saved to spotify_all_tracks.csv")

# ---------------- Save to MySQL ----------------
conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="root",
    database="spotify_db"
)
cursor = conn.cursor()

# Create table if not exists (only 5 columns)
cursor.execute("""
CREATE TABLE IF NOT EXISTS top_tracks (
    id INT AUTO_INCREMENT PRIMARY KEY,
    track_name VARCHAR(255),
    album VARCHAR(255),
    release_date DATE,
    popularity INT,
    duration_ms BIGINT,
    UNIQUE(track_name, album)  -- avoid duplicates
)
""")

insert_query = """
INSERT IGNORE INTO top_tracks 
(track_name, album, release_date, popularity, duration_ms)
VALUES (%s, %s, %s, %s, %s)
"""

cursor.executemany(insert_query, [
    (
        track['track_name'],
        track['album'],
        track['release_date'],
        track['popularity'],
        track['duration_ms']
    )
    for track in tracks_data
])

conn.commit()
cursor.close()
conn.close()

print("Data stored in MySQL successfully!")
