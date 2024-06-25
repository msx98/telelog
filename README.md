# Telegram Message Aggregator and Event Detector

This project aims to detect and summarize major events from a large stream of Telegram messages, with potential applications in market volatility prediction. 

## Intended Features

- **Real-time message aggregation:** Collects and embeds Telegram messages, incorporating a time decay mechanism.
- **Efficient event detection:** Clusters similar messages in a latent space and identifies significant event clusters.
- **Meaningful event labels:** Generates concise and informative labels for detected events by leveraging "trustworthy" messages within each cluster.
- **Scalable infrastructure:** Built with Python, Pyrogram, Postgres, pgvector, Docker, and utilizes multiple GPUs for efficient processing.

## Current Status

- Data pipeline and infrastructure for scraping, embedding, and processing Telegram messages are in place.
- A dataset of 23 million messages has been collected.
- Exploring various clustering and labeling techniques for optimal event detection.

## Future Goals

- Develop training tasks to fine-tune message embeddings for improved event detection and market relevance.
- Build a downstream market prediction model leveraging the encoded event information.
