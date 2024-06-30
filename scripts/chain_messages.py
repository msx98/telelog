#!/usr/bin/env python3
import logging
import sys
import os
import pandas as pd
import numpy as np


def get_output_path(input_path):
    input_path_no_ext, ext = os.path.splitext(input_path)
    output_path = f"{input_path_no_ext}_chain{ext}"
    return output_path


def perform_chain(input_path):
    chats_df = pd.read_parquet("output/chats.parquet")
    linked_chats_df = chats_df[["chat_id", "linked_chat_id"]]
    output_path = get_output_path(input_path)
    df = pd.read_parquet(input_path)
    df = (
        df.merge(linked_chats_df, how="left", on=["chat_id"])
        .assign(sent_by_linked_chat=lambda x: x["sender_id"] == x["linked_chat_id"])
        .drop("linked_chat_id", axis=1)
    )
    n = len(df)
    print(f"Working on {n} rows. Writing into {output_path}")
    #df.set_index(["chat_id", "message_id"], inplace=True, append=True, drop=False)
    df["forward_from_chat_id"] = np.nan # FIXME hack
    df["forward_from_message_id"] = np.nan # FIXME hack
    df = df.rename(columns={
        "reply_to_message_id": "parent_message_id"
    })
    df["chain"] = df["text"]
    parents_df = (
        df[df["parent_message_id"].notna()]
        #.drop("chat_id",axis=1)
        .drop("message_id",axis=1)
        .rename(columns={"parent_chat_id": "chat_id", "parent_message_id": "message_id"})
        [["chat_id", "message_id"]]
        .drop_duplicates()
        .assign(leaf=False)
    )
    df = df.merge(parents_df, on=["chat_id", "message_id"], how="left")
    df["leaf"] = df["leaf"].fillna(True)
    leaf_df = (
        df[df["leaf"]]
        .rename(columns={
            "message_id": "last_message_id",
        })
        .assign(
            chain_len=1,
            first_message_id=lambda x: x["last_message_id"],
        )
        [["chat_id", "sent_by_linked_chat", "first_message_id", "last_message_id", "chain", "chain_len", "parent_message_id"]]
    )
    schema_df = leaf_df.drop("parent_message_id", axis=1).iloc[:0]
    schema_df.to_parquet(output_path, engine="fastparquet")
    non_leaf_df = (
        df[~df["leaf"]]
        [["chat_id", "sent_by_linked_chat", "message_id", "text", "parent_message_id"]]
    )
    expect_size = len(leaf_df)
    while not leaf_df.empty:
        print(f"Leaves: {len(leaf_df)}")
        leaf_parents_df = (
            leaf_df.merge(non_leaf_df,
                          how="left",
                          left_on=["chat_id", "parent_message_id"], right_on=["chat_id", "message_id"])
            # FIXME - handle forwards by moving edges to the forwarded message
            .drop("parent_message_id_x", axis=1).rename(columns={"parent_message_id_y": "parent_message_id"})
            .assign(sent_by_linked_chat=lambda x: x["sent_by_linked_chat_x"] | x["sent_by_linked_chat_y"])
            .drop(["sent_by_linked_chat_x", "sent_by_linked_chat_y"], axis=1)
            #[["chat_id", "last_message_id", "chain_x", "chain_y", "parent_message_id_y"]]
        )
        orphan_mask = leaf_parents_df["message_id"].isna()
        done_df = leaf_parents_df[orphan_mask][schema_df.columns]
        done_df.to_parquet(output_path, engine="fastparquet", append=True)
        print(done_df)
        print(f"Wrote {len(done_df)}")
        leaf_parents_df = leaf_parents_df.loc[leaf_parents_df["message_id"].notna()]
        if leaf_parents_df.empty:
            break
        leaf_parents_df["chain"] = leaf_parents_df["chain"] + "<<NEXTMSG>>" + leaf_parents_df["text"]
        leaf_parents_df["chain_len"] = leaf_parents_df["chain_len"] + 1
        leaf_parents_df["first_message_id"] = leaf_parents_df["message_id"]
        leaf_parents_df.drop(["message_id", "text"], axis=1, inplace=True)
        #chain=lambda x: x.apply(lambda row: np.concatenate([row["chain"], [row["message_id"]]]), axis=1),
        leaf_df = leaf_parents_df


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logging.error(f"Usage: {sys.argv[0]} <input_path>")
        sys.exit(1)
    file_path = sys.argv[1]
    if not os.path.exists(file_path):
        logging.error(f"File not found: {file_path}")
        sys.exit(1)
    perform_chain(file_path)
