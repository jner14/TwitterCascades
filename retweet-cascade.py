import pandas as pd
import argparse
import re
import numpy as np


if __name__ == "__main__":

    # Arg parsing allows passing parameters via the command line
    parser = argparse.ArgumentParser(description="Extract re-tweet cascades from twitter data.")
    parser.add_argument('-i', dest='fileIN', help="name of file to process", default="alldays.csv")
    parser.add_argument('-o', dest='fileOUT', help="name of file to output", default="Re-tweet Cascade.csv")
    p_args = parser.parse_args()

    # Load dataset into a DataFrame
    df = pd.DataFrame()
    try:
        df = pd.read_csv(p_args.fileIN, index_col=0, parse_dates=["publicationTime"])
    except Exception as e:
        print(e)
        exit()

    # Remove links from bodyText
    df.bodyText = df.bodyText.apply(lambda x: re.sub("[^A-Za-z]http.*", "", x))

    # Replace newlines with period-space
    df.bodyText = df.bodyText.apply(lambda x: x.replace("\n", ". "))

    # Determine if it is a re-tweet
    df.insert(4, 'Retweet', df.bodyText.apply(lambda x: 0 if re.match("RT @[^:]*: ", x) is None else 1))

    # Determine who was the root user
    df.insert(0, 'Root', df.bodyText.apply(
        lambda x: re.match("RT @[^:]*: ", x).group()[3:-2] if re.match("RT @[^:]*: ", x) is not None else ""))

    # Save original message in new column
    df['FirstTweet'] = df.bodyText.apply(lambda x: re.sub("RT @[^:]*: ", "", x))

    # Sort first by FirstTweet and then by Retweet
    df.sort_values(['FirstTweet', 'publicationTime'], inplace=True)

    # Add an id field
    df.insert(0, "CascadeID", 0.0)
    # msk = df.Retweet == 0
    msk = df.FirstTweet != df.shift().FirstTweet
    df.loc[msk, "CascadeID"] = range(len(msk))
    df.loc[~msk, "CascadeID"] = [np.nan]*len(msk)
    df.CascadeID.ffill(axis=0, inplace=True)
    df.CascadeID = df.CascadeID.astype(dtype=np.int64)

    # Add re-tweet count
    df.insert(1, 'Size', -1)
    msk = df.CascadeID != df.shift().CascadeID
    df.loc[msk, "Size"] = df.groupby('CascadeID', sort=False).CascadeID.count().values

    # # Replace bodyText for re-tweets
    # df.loc[~msk, "bodyText"] = df.loc[~msk, "CascadeID"]
    # df.loc[~msk, "bodyText"] = df.loc[~msk, "bodyText"].apply(lambda x: "Cascade {}".format(x))

    # Fill empty Root values
    # grouped = df.loc[(df.Root == "")].groupby('CascadeID', sort=False).first()
    # df.loc[(df.Size > 0), "Root"] = grouped.Root.values
    df.loc[(df.Root == ""), "Root"] = df.loc[(df.Root == ""), "author"]

    # Save Data to file
    df.loc[msk, ["CascadeID", "Size", "Root", "bodyText"]].to_csv(p_args.fileOUT, index=False)

    print("Finished")
