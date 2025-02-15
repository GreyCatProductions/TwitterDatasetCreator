import os
import pandas as pd
from Data_Extractor import extract_amount_of_spreading_users_followers, extract_average_node_degrees, \
    extract_misinformation_spreading_rates


def main(hashtag_dir: str):
    hashtag_name = hashtag_dir.split('/')[-1]
    if not os.path.exists(hashtag_dir):
        print(f"{hashtag_dir} does not exist, exiting.")
        return

    dataset_files = os.listdir(hashtag_dir)

    for file_name in dataset_files:
        dataset_path = os.path.join(hashtag_dir, file_name)
        print(f"Trying to create dataset for: {file_name}")

        data = {
            "context": file_name,
            "network_type": True,
            "number_of_nodes": extract_amount_of_spreading_users_followers(dataset_path),
            "number_of_bots_and_authorities": 0,
            "average_node_degree": extract_average_node_degrees(dataset_path),
            "small_world": True,
            "always_connected": True,
            "directed": True,
            "initial_outbreak_size": 1,
            "misinformation_spreading_rates (og_no_like, og_like, all_no_like_mean, all_like_mean)": extract_misinformation_spreading_rates(dataset_path)
        }

        df = pd.DataFrame([data])
        csv_filename = f"dataset_{file_name.replace(' ', '_')}.csv"
        dir_name = f"{hashtag_name}_data"
        os.makedirs(dir_name , exist_ok=True)
        save_loc = os.path.join(dir_name , csv_filename)
        df.to_csv(save_loc, index=False, header=True)

        print(f"{csv_filename} successfully created!")


if __name__ == "__main__":
    paths_to_create_datasets_for = ["USAID Auflösung 05_02_25", "Alice Weidel 17_01_25", "Merz Habeck Merkel 31_01_25"]
    for path in paths_to_create_datasets_for:
        main(path)
