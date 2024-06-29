import unittest
from unittest.mock import patch
import numpy as np
import pandas as pd
from numpy import nan

from chain_messages import perform_chain


class TestChainMessages(unittest.TestCase):

    @patch("chain_messages.get_output_path")
    def test_perform_chain(self, mock_get_output_path):
        x = nan
        df_input = pd.DataFrame([
            # chat_id, message_id, reply_to_message_id, forward_from_chat_id, forward_from_message_id
            (1, 0, x, x, x),
            (1, 1, x, x, x),
            (1, 2, 1, x, x),
            (1, 3, x, x, x),
            (1, 4, 3, x, x),
            (1, 5, 4, x, x),
            (1, 6, 5, x, x),
            (1, 7, 3, x, x),
            (1, 8, 1, x, x),
            (1, 9, 4, x, x),
            (2, 0, x, 1, 2),
            (2, 1, x, x, x),
        ], columns=["chat_id", "message_id", "reply_to_message_id", "forward_from_chat_id", "forward_from_message_id"])
        df_input["text"] = df_input["message_id"].astype(str)
        df_output = pd.DataFrame([
            # chat_id, chain
            (1, 0, x, x, x),
            (1, 1, x, x, x),
            (1, 2, 1, x, x),
            (1, 3, x, x, x),
            (1, 4, 3, x, x),
            (1, 5, 4, x, x),
            (1, 6, 5, x, x),
            (1, 7, 3, x, x),
            (1, 8, 1, x, x),
            (1, 9, 4, x, x),
            (2, 0, x, 1, 5),
            (2, 1, x, x, x),
            (2, 9, 0, x, x),
        ], columns=["chat_id", "message_id", "reply_to_message_id", "forward_from_chat_id", "forward_from_message_id"])
        df_input["text"] = df_input["chat_id"].astype(str) + "," + df_input["message_id"].astype(str)
        df_input.to_parquet("output/test.parquet", engine="fastparquet")
        mock_get_output_path.return_value = "output/test_chain.parquet"
        perform_chain("output/test.parquet")
        df_output = pd.read_parquet("output/test_chain.parquet")
        self.assertEqual(len(df_output), len(df_input))
        self.assertEqual(
            sorted(df_output["chat_id"].tolist()),
            df_input["chat_id"].tolist()
        )


if __name__ == "__main__":
    unittest.main()