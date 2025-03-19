import os
import re
import pandas as pd


def process(paths, add_counters, create_summaries):
    for path in paths:
        if not os.path.exists(path):
            print(f"{path} does not exist")
            return
        if add_counters:
            count_entries(path)
        if create_summaries:
            summarize(path)


def count_entries(path: str):
    dataset_files = os.listdir(path)

    for file_name in dataset_files:
        dataset_path = os.path.join(path, file_name)
        if re.search(r"_\(\d+\)$", dataset_path):
            continue

        hour_dirs = os.listdir(dataset_path)
        files_sorted = sorted(
            [d for d in hour_dirs if d[:-1].isdigit()],
            key=lambda x: int(x[:-1])
        )

        if not files_sorted:
            print(f"No valid hour directories found in {dataset_path}")
            continue

        last_hour_dir = os.path.join(dataset_path, files_sorted[-1])
        total = os.path.join(last_hour_dir, "total.csv")

        if not os.path.exists(total):
            print(f"File not found: {total}")
            continue

        total_df = pd.read_csv(total)
        size = len(total_df)

        new_name = f"{dataset_path}_({size})"
        if dataset_path != new_name:
            os.rename(dataset_path, new_name)
            print(f"Summarized {file_name}")


def summarize(path: str):
    dataset_files = os.listdir(path)

    for file_name in dataset_files:
        dataset_path = os.path.join(path, file_name)
        hour_dirs = os.listdir(dataset_path)
        files_sorted = sorted(
            [d for d in hour_dirs if d[:-1].isdigit()],
            key=lambda x: int(x[:-1])
        )

        if not files_sorted:
            print(f"No valid hour directories found in {dataset_path}")
            continue

        last_hour_dir = os.path.join(dataset_path, files_sorted[-1])
        total = os.path.join(last_hour_dir, "total.csv")

        if not os.path.exists(total):
            print(f"File not found: {total}")
            continue

        df = pd.read_csv(total)

        # Ensure all required columns exist
        required_columns = ['view_count', 'like_count', 'reply_count', 'repost_and_quote_count', "reply_to_url", "quote_to_url", "post_url", "user_url", "like_affected_spreading_rate"]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            print(f"Skipping {total} - Missing columns: {missing_columns}")
            continue

        original_post = df[(df['reply_to_url'].isna() | (df['reply_to_url'] == "")) &
                           (df['quote_to_url'].isna() | (df['quote_to_url'] == ""))]

        df['like_affected_spreading_rate'] = df['like_affected_spreading_rate'].astype(str).str.rstrip(
            '%').astype(float)

        og_post_url = original_post['post_url'].values[0]

        data_row = pd.DataFrame([{
            "post_url": og_post_url,
            "user_url": original_post['user_url'].values[0],
            "spreading_rate in %": df['like_affected_spreading_rate'].mean(),
            "og_post_views": original_post['view_count'].values[0],
            "total_views": df['view_count'].sum(),
            "og_post_likes": original_post['like_count'].values[0],
            "total_likes": df['like_count'].sum(),
            "og_post_replies_with_spread": (df["reply_to_url"] == og_post_url).sum(),
            "og_post_replies": original_post['reply_count'].values[0],
            "total_replies_with_spread (also replies to quotes)": df['post_url'].count() - 1,
            "total_replies:": df['reply_count'].sum(),
            "og_post_quote_and_retweets": original_post['repost_and_quote_count'].values[0],
            "total_quote_and_retweets": df['repost_and_quote_count'].sum()
        }])

        out_path = os.path.join(dataset_path, "summary.csv")
        data_row.to_csv(out_path, index=False)  # Overwrite existing summary
        print(f"Summary created for {file_name}")