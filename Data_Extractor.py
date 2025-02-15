import os
import pandas as pd


def extract_start_hour(dataset_path: str) -> int:
    first_hour = os.listdir(dataset_path)[0].replace('h', '')
    return int(first_hour)

def extract_amount_of_spreading_users_followers(dataset_path: str) -> list[int]:
    followers = []

    for hour in os.listdir(dataset_path):
        users = os.path.join(dataset_path, hour, 'users.csv')

        def convert_followers(value: str) -> int:
            value = value.replace(" Followers", "").replace(",", "")
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
    :return: List of tuples of form (avg_followers, avg_following)
    """
    average_node_degrees = []

    for hour in os.listdir(dataset_path):
        users = os.path.join(dataset_path, hour, 'users.csv')

        def convert_followers(value: str) -> int:
            value = value.replace("Followers", "").replace(",", "")
            value = value.replace("Following", "").replace(",", "")
            if "K" in value:
                return int(float(value.replace("K", "")) * 1_000)
            elif "M" in value:
                return int(float(value.replace("M", "")) * 1_000_000)
            else:
                return int(float(value))

        df = pd.read_csv(users)
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

    for hour in os.listdir(dataset_path):
        data = os.path.join(dataset_path, hour, 'total.csv')

        if not os.path.exists(data):
            print(f"Skipping missing file: {data}")
            continue

        df = pd.read_csv(data)

        for col in ['dir spreading_rate', 'dir like affected spreading_rate']:
            df[col] = df[col].astype(str).str.replace('%', '').astype(float)

        og_post = df[(df['reply to url'].isna()) & (df['quote to url'].isna())]

        og_post_spread_no_like = float(og_post['dir spreading_rate'].sum())
        og_post_spread_with_like = float(og_post['dir like affected spreading_rate'].sum())
        all_posts_mean_spread_no_like = float(df['dir spreading_rate'].mean().round(5))
        all_posts_mean_spread_with_like = float(df['dir like affected spreading_rate'].mean().round(5))

        spreading_rates.append((
            og_post_spread_no_like,
            og_post_spread_with_like,
            all_posts_mean_spread_no_like,
            all_posts_mean_spread_with_like
        ))

    return spreading_rates
