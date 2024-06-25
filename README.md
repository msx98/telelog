# Telegram Message Aggregator and Hype Predictor

This project aims to detect and summarize major events from a large stream of Telegram messages, with potential applications in market volatility prediction. 

## Core Idea

Design an aggregator that effectively detects and summarizes events from a stream of messages, considering both semantic similarity and temporal relationships.

### Formal Definitions

- A message `m` is represented by its attributes: `m.date`, `m.text`, and a "hype" score `m.hype` derived from factors like views, reactions, and forwards (`m.hype ~= (m.views, m.reactions, m.forwards)`). 
- A message encoder `Em(m)` processes a message `m` and produces a vector representation `e`.
- An event encoder `Ev(e1, ..., ek)` takes a set of message embedding vectors (`e1` to `ek`) and produces a "summary" `s`, also characterized by a vector representation.
- An aggregator `A(m1, ..., mn)` processes a set of messages (`m1` to `mn`) and outputs a set of summaries `s1, ..., sk` representing detected events (where `k <= n`, determined dynamically by `A`).

### Challenges and Considerations

- **Temporality:** Events unfold over time. A robust system needs to understand that events occurring close in time might be related, while those further apart might not be. For real-time applications, distinguishing between similar events happening at different times is crucial. 
- **Hype Detection:**  Accurately quantifying the "importance" or "hype" of a message is vital for filtering noise and focusing on significant events.

## Existing Features

- **Data Collection:**  A system is in place to scrape Telegram messages and their metadata (forwards, reactions, views, etc.) into a Postgres database. It supports concurrent client connections.
- **Message Embedding Pipeline:**  Given an embedding model, the system can efficiently generate embeddings for messages. These embeddings are then used to update the database. A queueing mechanism ensures that the database is not overloaded.
- **RAG:**  Retrieval Augmented Generation (RAG) is implemented, enabling a chatbot to access and utilize the information stored in the database using pgvector.
- **Dataset:**  A large dataset of Telegram messages has been collected and is available for model training and evaluation.

## Approaches

### Baseline

1. **Message Encoding:** Use a pre-trained language model (like BERT) as the message encoder (`Em`).
2. **Event Encoding:**  Define the event encoder (`Ev`) as a simple average of the message embeddings within a given cluster.
3. **Aggregation:** 
    -  Filter messages within a recent time window (e.g., past 24 hours) and have a hype score above the sender's average.
    - Apply HDBSCAN clustering on the filtered message embeddings.
    - The cluster centers represent potential events.
4. **Event Summarization:** For each cluster center:
    -  Identify the `l` (e.g., `l = 5`) closest messages.
    -  Generate a summary, using a LLM or cheap NER.

### Potential Improvements

- **Fine-tuning:** Further train the initial message encoder (`Em`) and tokenizer on message trees (i.e. messages and their responses) extracted from the dataset to improve their performance on this specific task.
- **Time-Aware Embeddings:**  Incorporate temporal information directly into the message embeddings. For instance, create a new encoder `Em'(m)` that combines the message metadata with the standard message embedding (`Em(m)`), perhaps using another encoder. `Em'(m) = f(Em(m), m.date, m.hype)`. Dimensionality might be an issue? (a handful of metadata features would be attenuated by the large embedding size)
- **Attention-Based Aggregation:**  Instead of clustering, we can explore an attention-like mechanism. Here, the relevance of a message (and its embedding) to a potential event is influenced by not only semantic similarity, but the hype score (which affects `V`) also temporal proximity (which affects `K*V`).
It does leave the question of how we might train the model to do that without replacing `MatMul` with something else, which could ruin computability.
- **Training Objective:** 
    -  Train the model to predict the hype of an event based on its aggregated representation.
    -  Use a feedforward neural network (FFNN) on top of each of the aggregator's outputs.
    -  Penalize the model based on the deviation of its predicted hype from the actual aggregated hype of the messages in the cluster.
- **Change Detection:** Calculate the difference in cluster formations between consecutive time steps. Significant changes could signal the emergence of new events.
- **Per-Chat Aggregation:** Run the pipeline separately for each chat, and then develop a method to aggregate these per-chat event summaries into a global overview.
- **Mass-based Clustering:** Two rocket attacks occurring at 8 AM and 9 AM, respectively, need to be recognized as separate events for the sake of real-time alerts, even though they might be causally linked to a subsequent event like a war declaration at 10 AM. So we might not want to merge them at 9AM, but we would at 10AM. Maybe a weight-based clustering, i.e. adding something akin to a physical mass based on hype, and finding mass peaks could help tackle this.
