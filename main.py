import zipfile, pandas as pd


def main():
    zip_path = "data/gtfs.zip"
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall("data/gtfs")

    stops_df = pd.read_csv("data/gtfs/stops.txt")
    routes_df = pd.read_csv("data/gtfs/routes.txt")
    trips_df = pd.read_csv("data/gtfs/trips.txt")
    stop_times_df = pd.read_csv("data/gtfs/stop_times.txt")

    print("Stops DataFrame:")
    print(stops_df.head())
    print("\nRoutes DataFrame:")
    print(routes_df.head())
    print("\nTrips DataFrame:")
    print(trips_df.head())
    print("\nStop Times DataFrame:")
    print(stop_times_df.head())






if __name__ == "__main__":
    main()
