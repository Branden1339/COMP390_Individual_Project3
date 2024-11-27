import requests
import sqlite3
import json

# Geolocation bounding boxes for each region
bound_box_dict = {
    'Africa_MiddleEast_Meteorites': (-17.8, -35.2, 62.2, 37.6),
    'Europe_Meteorites': (-24.1, 38.0, 32.1, 71.1),
    'Upper_Asia_Meteorites': (33.0, 38.0, 190.4, 72.7),
    'Lower_Asia_Meteorites': (63.0, -9.9, 154.0, 37.6),
    'Australia_Meteorites': (112.9, -43.8, 154.3, -11.1),
    'North_America_Meteorites': (-168.2, 12.8, -52.0, 71.5),
    'South_America_Meteorites': (-81.2, -55.8, -34.4, 12.5)
}

# Function to check if a location is within the bounding box of a region
def is_within_bounds(lat, lon, region):
    min_lon, min_lat, max_lon, max_lat = bound_box_dict[region]
    return min_lon <= lon <= max_lon and min_lat <= lat <= max_lat

# Function to fetch meteorite data from NASA's API
def fetch_meteorite_data():
    url = "https://data.nasa.gov/resource/gh4g-9sfh.json"
    try:
        response = requests.get(url)
        # Check if the request was successful
        if response.status_code == 200:
            print("GET request successful!")
            return response.json()  # Convert response to JSON object (list of dictionaries)
        else:
            print(f"GET request failed! Status Code: {response.status_code} - {response.reason}")
            return None
    except requests.RequestException as e:
        print(f"An error occurred: {e}")
        return None

# Function to create tables in the SQLite database
def create_tables(conn):
    try:
        cursor = conn.cursor()
        # Create tables for each region
        for region in bound_box_dict:
            cursor.execute(f'''
                CREATE TABLE IF NOT EXISTS {region} (
                    id TEXT,
                    name TEXT,
                    mass TEXT,
                    year TEXT,
                    reclat TEXT,
                    reclong TEXT
                )
            ''')
        conn.commit()
    except sqlite3.DatabaseError as e:
        print(f"Database error occurred: {e}")

# Function to insert meteorite data into the appropriate table
def insert_data(conn, data):
    try:
        cursor = conn.cursor()
        for entry in data:
            try:
                name = entry.get('name', '')
                mass = entry.get('mass', '')
                year = entry.get('year', '')
                reclat = entry.get('reclat', '')
                reclong = entry.get('reclong', '')
                
                if reclat and reclong:
                    # Check the region for the meteorite based on its geolocation
                    for region in bound_box_dict:
                        if is_within_bounds(float(reclat), float(reclong), region):
                            cursor.execute(f'''
                                INSERT INTO {region} (id, name, mass, year, reclat, reclong)
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', (entry.get('id', ''), name, mass, year, reclat, reclong))
                            break
            except KeyError:
                print("Skipping incomplete data entry.")
        conn.commit()
    except sqlite3.DatabaseError as e:
        print(f"Error inserting data into the database: {e}")

# Main function
def main():
    # Fetch meteorite data from NASA
    data = fetch_meteorite_data()
    if data:
        # Connect to the SQLite database
        try:
            conn = sqlite3.connect('meteorites.db')
            create_tables(conn)
            insert_data(conn, data)
            print("Data successfully inserted into the database!")
        except sqlite3.DatabaseError as e:
            print(f"Failed to connect to the database: {e}")
        finally:
            conn.close()

if __name__ == "__main__":
    main()


