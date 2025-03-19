import os
import pandas as pd


def extract_start_hour(dataset_path: str) -> int:
    files = os.listdir(dataset_path)
    files_sorted = sorted(
        [d for d in files if d[:-1].isdigit()],
        key=lambda x: int(x[:-1])
    )
    return int(files_sorted[0][:-1])

def extract_amount_of_spreading_users_followers(dataset_path: str) -> list[int]:
    followers = []
    hours = os.listdir(dataset_path)
    hours_sorted = sorted(
        [d for d in hours if d[:-1].isdigit()],
        key=lambda x: int(x[:-1])
    )
    for hour in hours_sorted:
        users = os.path.join(dataset_path, hour, 'users.csv')

        def convert_followers(value: str) -> int:
            value = value.replace("Followers", "").replace(",", "")
            if "K" in value:
                return int(float(value.replace("K", "")) * 1_000)
            elif "M" in value:
                return int(float(value.replace("M", "")) * 1_000_000)
            else:
                return int(float(value))

        df = pd.read_csv(users)
        df["followers_count"] = df["followers_count"].apply(convert_followers)
        total_followers = int(df["followers_count"].sum())
        followers.append(total_followers)
    return followers

def extract_average_node_degrees(dataset_path: str) -> list[tuple[int, int]]:
    """
    Extracts and prints the average number of followers and following per hour.

    :param dataset_path: Path to the dataset directory.
    :return: List of tuples of form (avg_followers, avg_following).
    """
    average_node_degrees = []

    hours = os.listdir(dataset_path)
    hours_sorted = sorted(
        [d for d in hours if d[:-1].isdigit()],
        key=lambda x: int(x[:-1])
    )
    for hour in hours_sorted:
        users_file = os.path.join(dataset_path, hour, 'users.csv')

        def convert_followers(value: str) -> int:
            value = value.replace("Followers", "").replace(",", "")
            value = value.replace("Following", "").replace(",", "")
            if "K" in value:
                val = int(float(value.replace("K", "")) * 1_000)
                return val
            elif "M" in value:
                val = int(float(value.replace("M", "")) * 1_000_000)
                return val
            else:
                val = int(float(value))
                return val

        df = pd.read_csv(users_file)
        df["followers_count"] = df["followers_count"].apply(convert_followers)
        df["following_count"] = df["following_count"].apply(convert_followers)

        avg_followers = int(df["followers_count"].mean())
        avg_following = int(df["following_count"].mean())

        average_node_degrees.append((avg_followers, avg_following))

    return average_node_degrees

def extract_misinformation_spreading_rates(dataset_path: str) -> list[tuple[float, float, float, float]]:
    """
    :return: List of tuples in the form:
             (og_post_no_like_spread, og_post_like_spread, all_posts_mean_spread_no_like, all_posts_mean_spread_like)
    """
    spreading_rates = []
    hours = os.listdir(dataset_path)
    hours_sorted = sorted(
        [d for d in hours if d[:-1].isdigit()],
        key=lambda x: int(x[:-1])
    )
    for hour in hours_sorted:
        try:
            data = os.path.join(dataset_path, hour, 'total.csv')

            if not os.path.exists(data):
                print(f"Skipping missing file: {data}")
                continue

            df = pd.read_csv(data)

            for col in ['spreading_rate', 'like_affected_spreading_rate']:
                df[col] = df[col].astype(str).str.replace('%', '').astype(float)

            og_post = df[(df['reply_to_url'].isna()) & (df['quote_to_url'].isna())]

            og_post_spread_no_like = float(og_post['spreading_rate'].sum())
            og_post_spread_with_like = float(og_post['like_affected_spreading_rate'].sum())
            all_posts_mean_spread_no_like = float(df['spreading_rate'].mean().round(5))
            all_posts_mean_spread_with_like = float(df['like_affected_spreading_rate'].mean().round(5))

            spreading_rates.append((
                og_post_spread_no_like,
                og_post_spread_with_like,
                all_posts_mean_spread_no_like,
                all_posts_mean_spread_with_like
            ))
        except Exception as e:
            print(f"failed to extract spreading rate for {dataset_path}")
            print(e)
            return []
    return spreading_rates
