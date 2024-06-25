# Auto-Journalist

Essentially a social media aggregator, taking a large number of messages and placing a small number of points (with time decay) on a latent space, and cheaply generating meaningful labels (e.g. "rockets;Ashdod", "TEVA;buyout", etc) using "trustworthy" messages near each point.
The end goal is to use this as the basis for a market prediction model. I think that given a good encoder, we can at least predict volatility, even if latent space features are not necessarily market-oriented. Eventually, I'd also like to find training tasks that can tune the encodings towards something that retains market-relevant information.
