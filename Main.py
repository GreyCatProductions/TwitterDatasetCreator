import os
import pandas as pd
from Data_Extractor import extract_amount_of_spreading_users_followers, extract_average_node_degrees, \
    extract_misinformation_spreading_rates, extract_start_hour


def main(hashtag_dir: str):
    hashtag_name = hashtag_dir.split('/')[-1]
    if not os.path.exists(hashtag_dir):
        print(f"{hashtag_dir} does not exist, exiting.")
        return

    dataset_files = os.listdir(hashtag_dir)

    for file_name in dataset_files:
        dataset_path = os.path.join(hashtag_dir, file_name)
        print(f"Trying to create dataset for: {file_name}")

        start_hour = extract_start_hour(dataset_path)
        spreading_rates = extract_misinformation_spreading_rates(dataset_path)
        number_of_nodes = extract_amount_of_spreading_users_followers(dataset_path)
        avg_node_degrees = extract_average_node_degrees(dataset_path)
        data_rows = []

        counter = 0
        for hour in range(start_hour, start_hour + len(spreading_rates)):
            data_rows.append({
                "hour": hour,
                "network_type": True,
                "number_of_nodes (sum of all followers of spreading users)": number_of_nodes[counter],
                "number_of_bots_and_authorities": 0,
                "average_node_degree (mean_followers, mean_following)": avg_node_degrees[counter],
                "small_world": True,
                "always_connected": True,
                "directed": True,
                "initial_outbreak_size": 1,
                "misinformation_spreading_rate (og_no_like, og_like, all_mean_no_like, all_mean_like) in %": spreading_rates[counter],
            })
            counter += 1

        df = pd.DataFrame(data_rows)
        csv_filename = f"dataset_{file_name.replace(' ', '_')}.csv"
        dir_name = f"{hashtag_name}_data"
        os.makedirs(dir_name, exist_ok=True)
        save_loc = os.path.join(dir_name, csv_filename)
        df.to_csv(save_loc, index=False, header=True)

        print(f"{csv_filename} successfully created!")

if __name__ == "__main__":
    paths_to_create_datasets_for = ["USAID Auflösung 05_02_25", "Alice Weidel 17_01_25", "Merz Habeck Merkel 31_01_25"]
    for path in paths_to_create_datasets_for:
        main(path)
