# Telegram Message Aggregator and Hype Predictor

This project aims to detect and summarize major events from a large stream of Telegram messages, with potential applications in market volatility prediction. 

## Existing Features
- **Data Collection:** Scrapes Telegram messages along with their metadata (forwards, reactions, views etc) into a Postgres database. Support for concurrent clients.
- **Message Embedding Pipeline:** Given an embedding model, can efficiently embed messages in a remote server and update the database with no throttling on the DB side using a queue.
- **RAG:** ChatBot allows RAG thanks to pgvector.
- **Dataset:** A very large dataset of messages has been collected.

## Core Idea

Design an aggregator that effectively detects and summarizes events from a stream of messages, considering both semantic similarity and temporal relationships.

### Formal Definitions
A message `m` is represented by `m.date`, `m.text`, `m.hype ~= (m.views, m.reactions, m.forwards)`, etc.
A message encoder `Em(m)` produces a vector representation `e` of the message.
An event encoder `Ev(e1, ..., ek)` receives a set of message embedding vectors, and produces a "summary" `s`, characterized by a vector representation.
An aggregator `A(m1, ..., mn)` receives a set of messages, and produces a set of summaries `s1, ..., sk` which we call events (k <= n, and A picks k dynamically).

### Approaches
- A really naive one would be to mindlessly shove everything into a SOTA LLM and ask it to summarize everything it sees, maybe with a sliding time window and while filtering out low-hype messages using a manual threshold.
- A slightly less naive approach, would be to use a pretrained `Em` (e.g. language translation model -> BERT), then define `Ev := AVERAGE`, and `A(m1, ..., mn) := HDBSCAN({mi : mi.date >= now - 1day /\ mi.hype >= mi.sender.average_hype}).cluster_centers`, then set the label of each event as some summary of the `l = 5` closest messages to the cluster center, using a LLM or through NER.
A time dimension can be concatenated to the message embeddings, and define something like `Em'(m) := AnotherEncoder(m.date, Em(m))`.
- Instead of clustering, we can use a mechanism similar to attention, where the `K * V` result of two embeddings is affected by temporal proximity, and strengthened by the hype score of the message associated with `V`. This is a delicate point though, because how do we actually train the model to do that?
- We could perhaps train our model to predict hype from events (using a FFNN on top of each of the aggregator's outputs), and penalize it based on deviation from actual cluster hype.
- To detect changes, we could measure the difference between two timesteps, and mark new clusters.
- We can also run this pipeline for each chat, and then try to aggregate the results somehow.

### Note on Temporality
One rocket attack at 8AM and one at 9AM are not the same event for the sake of real-time alerts.
Nonetheless, if a war erupts at 10AM, a reasonable person would infer some causal relationship between the attacks and the war.
For the same of clustering, I can think of two ways we can go about this:
