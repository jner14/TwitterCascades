import pandas as pd
import argparse
import re
import twitter
import twitterCreds as creds

api = twitter.Api(consumer_key          = creds.consumer_key,
                  consumer_secret       = creds.consumer_secret,
                  access_token_key      = creds.access_token_key,
                  access_token_secret   = creds.access_token_secret,
                  sleep_on_rate_limit   = True)

if __name__ == "__main__":

    # Arg parsing allows passing parameters via the command line
    parser = argparse.ArgumentParser(description="Extract follower cascades from twitter data.")
    parser.add_argument('-i', dest='fileIN', help="name of file to process", default="alldays.csv")
    parser.add_argument('-o', dest='fileOUT', help="name of file to output", default="Follower Cascade.csv")
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

    # Sort by date
    df.sort_values(['publicationTime'], inplace=True)

    # Remove unnecessary parts of id values
    df.author = df.author.apply(lambda x: int(x.replace("id:twitter.com:", "")))

    # Determine if it is a re-tweet
    df.insert(0, 'Retweet', df.bodyText.apply(lambda x: 0 if re.match("RT @[^:]*: ", x) is None else 1))

    # Get followers
    c_id = 0
    df['Members'] = ""
    df['ScreenName'] = ""
    df['CascadeID'] = -1
    df['Size'] = 0
    df['StartTime'] = ""
    df['EndTime'] = ""
    followed = []
    allFollowers = []
    i = 0
    for k, row1 in df.iterrows():
        if row1.author in followed or row1.author in allFollowers:
            i += 1
            continue
        df.loc[k, "CascadeID"] = c_id
        followed.append(row1.author)
        df.loc[k, 'Members'] += "{},".format(row1.author)
        df.loc[k, 'Size'] += 1
        df.loc[k, 'StartTime'] = row1.publicationTime
        df.loc[k, 'EndTime'] = row1.publicationTime
        try:
            df.loc[k, "ScreenName"] = api.GetUser(user_id=row1.author).screen_name
        except Exception as e:
            print("{} {}".format(e, row1.author))
            df.loc[k, "ScreenName"] = "API ERROR - {}".format(row1.author)
            c_id += 1
            i += 1
            continue
        rowsFollowers = api.GetFollowerIDs(user_id=row1.author)
        for k2, row2 in df.iterrows():
            # if row2 author is not following or being followed then add to row1 author Members
            if row2.author not in allFollowers and row2.author not in followed and row2.author in rowsFollowers:
                df.loc[k, 'Members'] += "{},".format(row2.author)
                df.loc[k, 'bodyText'] += "{} | ".format(row2.bodyText)
                df.loc[k, 'EndTime'] = row2.publicationTime
                df.loc[k, 'Size'] += 1
                allFollowers.append(row2.author)

        # Remove last comma
        df.loc[k, 'Members'] = df.loc[k, 'Members'][:-1]

        # Save Data to file
        df.loc[(df.CascadeID > -1), ["CascadeID",
                                     "Size",
                                     "ScreenName",
                                     "StartTime",
                                     "EndTime",
                                     "bodyText",
                                     "Members"]].to_csv(p_args.fileOUT, index=False)

        print("Row={}, CascadeID={}".format(i, c_id))
        c_id += 1
        i += 1

    print("Finished")
