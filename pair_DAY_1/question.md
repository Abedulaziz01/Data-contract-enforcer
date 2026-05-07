# Peer Research Question

In [`contracts/ai_extensions.py`](C:/Users/user/Desktop/mll/week7/Data-contract-enforcer/contracts/ai_extensions.py), the embedding drift check reduces up to 200 extracted fact texts into a single embedding centroid and then uses cosine distance against a saved baseline to decide whether semantic drift has occurred.

My question is:

**What does cosine distance between embedding centroids actually measure in a system like this, what kinds of semantic or distribution shifts can it miss, and how should I reason about whether centroid-based drift is a defensible production choice versus alternatives like per-sample distance distributions, clustering-based drift checks, or MMD-style tests?**

Why this gap matters to my work:

I can explain that the project has an "embedding drift" check, but I cannot yet defend the underlying statistical mechanism or the tradeoff behind using a centroid summary. Closing this gap would let me improve the explanation and design rationale for the AI extensions portion of my Data Contract Enforcer project, especially in the dashboard and README where I currently present the check as if its meaning were obvious.
