#!/usr/bin/env python3
import os
import pandas as pd
import numpy as np
pd.set_option('display.max_colwidth', None)

input_path = "output/hebrew_messages_2024-06-26_30d.parquet"

input_path_no_ext, ext = os.path.splitext(input_path)
output_path = f"{input_path_no_ext}_chain{ext}"

df = pd.read_parquet(input_path)
n = len(df)
print(f"Working on {n} rows. Writing into {output_path}")

df["is_reply"] = df["reply_to_message_id"].notna() & (df["forward_from_chat_id"].isna() | (df["forward_from_chat_id"] == df["chat_id"]))

df_chain = df
df_chain_end = df[~df["is_reply"]][["chat_id", "message_id", "text"]]
df_chain_end.to_parquet(output_path, engine="fastparquet")
df_chain_replies = df[df["is_reply"]]

# Keep going until no more replies
while True:
    if len(df_chain_replies) == 0:
        break
    print(f"Replies: {len(df_chain_replies)}")
    df_chain_replies_with_origin = (
        df_chain_replies[["chat_id", "message_id", "reply_to_message_id", "text"]]
        .merge(df_chain, how="left", left_on=["chat_id", "reply_to_message_id"], right_on=["chat_id", "message_id"])
    )
    lost_origin = df_chain_replies_with_origin.message_id_y.isna()
    addition_df = (
        df_chain_replies_with_origin[lost_origin][["chat_id", "message_id_x", "text_x"]]
        .rename(columns={"text_x": "text", "message_id_x": "message_id"})
    )
    #df_chain_end = pd.concat([df_chain_end, addition_df])
    addition_df.to_parquet(output_path, engine="fastparquet", append=True)
    df_chain_replies_with_origin.text_x = (
        df_chain_replies_with_origin["text_y"] + "<<NXTMSG>>" + df_chain_replies_with_origin["text_x"]
    )
    df_chain_replies_with_origin = (
        df_chain_replies_with_origin[~lost_origin]
        .drop(["message_id_x", "reply_to_message_id_x", "text_y"], axis=1)
        .rename(columns={"message_id_y": "message_id", "text_x": "text", "reply_to_message_id_y": "reply_to_message_id"})
        [["chat_id", "message_id", "text", "reply_to_message_id", "forward_from_chat_id", "forward_from_message_id", "is_reply"]]
    )
    df_chain_replies = df_chain_replies_with_origin[df_chain_replies_with_origin["is_reply"]]
    tail_df = df_chain_replies_with_origin[~df_chain_replies_with_origin["is_reply"].astype(bool)].copy()
    tail_df["message_id"] = tail_df["message_id"].astype(np.int64)
    tail_df["chat_id"] = tail_df["chat_id"].astype(np.int64)
    #df_chain_end = pd.concat([df_chain_end, tail_df])
    tail_df[["chat_id", "message_id", "text"]].to_parquet(output_path, engine="fastparquet", append=True)
    del tail_df

print("Success")