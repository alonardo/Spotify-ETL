import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
from sqlalchemy.types import Integer, Text, DateTime

load_dotenv()

class Pandas_ETL_Pipeline:
    client_id = os.environ.get('CLIENT_ID')
    client_secret = os.environ.get('CLIENT_SECRET')
    database_url = os.environ.get('DATABASE_URL')

    def get_data(self):
        sp_api_call = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=self.client_id,
            client_secret=self.client_secret, 
            redirect_uri='http://localhost:3000/callback',
            scope='user-library-read user-read-recently-played'))
        
        return sp_api_call.current_user_recently_played()

    def artists_helper(self, artlist):
        ans = [x['name'] for x in artlist]
        return ', '.join(ans)

    # we're creating an ETL process - extracting data, transforming data, and loading data
    def extract(self): 
        print('Extracting...')
        data = self.get_data()

        song_names = [x['track']['name'] for x in data['items']]
        
        artist_names = [self.artists_helper(x['track']['artists']) for x in data['items']]
        
        popularity = [x['track']['popularity'] for x in data['items']]
        
        played_at = [x['played_at'] for x in data['items']]

        songs_dict = {
            'song_name': song_names,
            'artist_names': artist_names,
            'popularity': popularity,
            'played_at': played_at
        }

        songs_df = pd.DataFrame(songs_dict, columns=['song_name', 'artist_names', 'popularity', 'played_at'])
        print(songs_df)
        print('Extraction Complete')
        return songs_df

    def popularity_categorize(self, pop_index):
        if pop_index < 25:
            return 'Unknown'
        elif pop_index < 50:
            return 'Low'
        elif pop_index < 75:
            return 'High'
        else:
            return 'Overplayed'

    def transform(self):
        songs = self.extract()        
        if songs.empty:
            print('User has not played any music.')
            return False
    
        if pd.Series(songs['played_at']).is_unique:
            print('....')

        else:
            raise Exception(f'[Error during Transformation]: Primary Keys duplicated. Double check data extraction to ensure data is not corrupted.')

        if songs.isnull().values.any():
            raise Exception('Null values found')

        print('Transforming data...')
    
        songs['popularity_category'] = songs['popularity'].apply(self.popularity_categorize)
        
        print(songs)
        return songs

    def load(self):
        data = self.transform()
        
        try:
            if not data:
                print('Error transforming...')
        except:
            print('Data loading....')
            
            dburl = os.environ.get('DATABASE_URL')

            data.to_sql('Recently_Played_Popularity', index=False, con=dburl, if_exists='append', schema='public', chunksize=500, dtype={
                'song_name': Text,
                'artist_names': Text,
                'popularity': Integer,
                'played_at': DateTime,
                'popularity_category': Text
            })

            print('Data load complete.')

etl = Pandas_ETL_Pipeline()
etl.load()