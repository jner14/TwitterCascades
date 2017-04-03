import pandas as pd
import argparse
import datetime as dt
import multiprocessing as mp


def matches_util(args):
    df_in, word_list, min_keywords = args

    # Check for every keyword in ever row
    values = pd.DataFrame()
    for kw in word_list:
        values[kw] = df_in.bodyText.str.contains(kw, case=False)

    # Add keyword counts
    values["wordCnt"] = [v.sum() for k, v in values.iterrows()]

    # If keyword count is greater than min_keywords then include in result
    return df_in.loc[values.wordCnt >= min_keywords]


def get_matches3(df_in, word_list, min_keywords=2, output_filepath="", multi_threads=False, thread_cnt=2):
    print("Looking for matches.\nmin_keywords={}\noutput_filepath={}\nmulti_threads={}\nword_list={}".format(
        min_keywords,
        output_filepath,
        multi_threads,
        word_list))

    # Make sure bodyText field exists
    assert "bodyText" in df_in.columns, "Field 'bodyText' was not found in input DataFrame!"
    ts1 = dt.datetime.now()
    result = None
    resList = None

    # Use multi-threading if enabled
    if multi_threads:
        with mp.Pool(thread_cnt) as p:
            threadCnt = thread_cnt
            bins = pd.cut(df_in.index, threadCnt, labels=range(threadCnt))
            resList = p.map(matches_util, ((df_in.loc[bins == i], keywords, min_keywords) for i in range(thread_cnt)))

    # Else process without multi-threading
    else:
        result = matches_util((df_in, keywords, min_keywords))

    # Get results from queue
    if result is None:
        result = pd.concat(resList)

    ft = dt.datetime.now() - ts1
    print("Finished looking for matches.  Time Elapsed={:.1f}s".format(ft.total_seconds()))

    # Save resulting matches to file if a filepath was passed
    if output_filepath != "":
        result.to_csv(output_filepath)
        print("Saved output to {}".format(output_filepath))

    return result


if __name__ == "__main__":

    # Arg parsing allows passing parameters via the command line
    parser = argparse.ArgumentParser(description="Extract re-tweet cascades from twitter data.")
    parser.add_argument('-i', dest='fileIN', help="name of file to process", default="short.csv")
    parser.add_argument('-o', dest='fileOUT', help="name of file to output", default="matches.csv")
    p_args = parser.parse_args()

    # Load dataset into a DataFrame
    df = None
    try:
        ts = dt.datetime.now()
        print("Attempting to load %s" % p_args.fileIN)
        df = pd.read_csv(p_args.fileIN, index_col=0)
        print("Finished loading {}. Time Elapsed={}".format(p_args.fileIN, dt.datetime.now() - ts))
    except Exception as e:
        print(e)

    assert df is not None, "Failed to load input file %s" % p_args.fileIN

    keywords = ["protest", "muslim", "islam", "outcry", "asylum", "organisation", "threat", "union", "centrelink",
                "opposition", "parade", "council", "federal", "strike", "harass", "refugee", "riot", "community",
                "reclaim", "poster", "demonstration", "petition", "funding", "barrier", "march", "crowd", "celebration",
                "action", "barricade", "placard", "gather", "resident", "patriot", "bigot", "racism", "national",
                "decision", "movement", "mentality", "racist", "agency", "mosque", "highlight", "halaal", "turmoil",
                "activist", "disturbance", "victory", "equality", "blockade", "anger", "ideal", "unite", "extremist",
                "anzac", "rally", "culture", "unrest", "terror", "terrorist", "prevent", "cpsu", "anger", "concern",
                "stoppage", "park", "value", "claim", "immigrant", "opponent", "standoff", "spray", "sign", "urban",
                "pressure", "destroy", "side", "unrest", "stoking"]

    get_matches3(df, word_list=keywords, min_keywords=2, output_filepath=p_args.fileOUT,
                 multi_threads=True, thread_cnt=8)
    print("Finished")
