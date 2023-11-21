import os
import shutil
import re
import tkinter
import json
import pandas as pd
import distutils

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from tkinter import filedialog

# Create spark session
spark = SparkSession.builder.master("local[*]").appName("list_duplicates").getOrCreate()

root = tkinter.Tk()
root.withdraw()

# source_folder = filedialog.askdirectory()
# target_folder = filedialog.askdirectory()

# source_folder = '/home/anderdam/DataEngineering/scripts/list_duplicates/source_dirs'
source_folder = "/home/anderdam/gdrive_downloads"
target_folder = (
    "/home/anderdam/DataEngineering/scripts/list_duplicates/source_dirs/target_dir"
)


files_dict = {
    "paths": [],
    "names": [],
    "sizes": [],
    "hashes": [],
}

while True:
    # Recursively search source folder
    for root, dirs, files in os.walk(source_folder):
        for file in files:
            full_dir = os.path.join(root, file)
            split_dir = full_dir.split("/")

            home = "/".join(split_dir[0:3])
            basedir = "/".join(split_dir[4:-1])
            filename = split_dir[-1]
            size = os.path.getsize(os.path.join(root, file))
            file_hash = hash(
                (os.path.basename(file), os.path.getsize(os.path.join(root, file)))
            )

            files_dict["paths"].append(basedir)
            files_dict["names"].append(filename)
            files_dict["sizes"].append(size)
            files_dict["hashes"].append(file_hash)

    pd.set_option("display.max.colwidth", None)
    df = pd.DataFrame(data=files_dict)

    ds = spark.createDataFrame(data=df)

    filtered_ds = (
        ds.groupBy("names", "sizes", "hashes")
        .agg(f.count("hashes").alias("duplicates"))
        .filter(f.col("duplicates") > 1)
        .orderBy(f.col("duplicates"))
    )
    new_df = filtered_ds.toPandas()

    limit = 1
    min_count = new_df["duplicates"].min()

    if min_count <= limit:
        break

    name = []
    out_file_hash = []
    for index, row in new_df.iterrows():
        name = row["names"]
        sizes = row["sizes"]
        out_file_hash = row["hashes"]
        num_duplicates = row["duplicates"]

    for root, dirs, files in os.walk(source_folder):
        for file in files:
            if file == name and hash(file) == out_file_hash:
                full_path = os.path.join(root, file)
                target_file = os.path.join(target_folder, name)
                print(f"Moving file {target_file}!")
                shutil.move(full_path, target_file)
