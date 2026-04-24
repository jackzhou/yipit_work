## Approach

Use pandas DataFrame APIs for data cleaning and transformation, SentenceTransformer for embedding generation, and `sklearn.metrics.pairwise` for similarity calculation.

## Choice of model

The all-MiniLM-L6-v2 model is used because it provides fast, lightweight sentence embeddings with strong semantic similarity performance, making it well-suited for comparing article titles and summaries. It runs efficiently in a local environment without external dependencies, supporting scalable batch processing and reliable execution.

## Design and implementation notes

### Data quality handling – company metadata

The provided `company_metadata.json` contains inconsistencies between the `is_public` and `stock_ticker` fields. For example, some companies marked as private (`is_public: false`) have non-null stock tickers (e.g., OpenAI with ticker `"OPEN"`).

**Resolution strategy**

- If `stock_ticker` is present and `is_public` is `false`, then:
  - `is_public` is set to `true` (the ticker is kept as given).

### On category and industry joins

No join is performed between the article category (from news data) and the company industry (from metadata) during ETL, as such a join may result in data loss. The data does not appear to exhibit strong relationships.

### Trade-off in retrieving top-K similar articles

When retrieving the top three similar articles for a query, similarity is computed only within the filtered dataset rather than across the entire corpus. This may result in a slight reduction in retrieval quality, as potentially more relevant articles outside the filtered subset are not considered. However, this approach significantly reduces computational cost by avoiding full pairwise cosine similarity calculations, providing a practical balance between performance and accuracy. When a user query is presented, low latency is assumed to be a priority.

### Invalid company names

Unmatched company names are logged; however, all company names in the news dataset are found in the provided metadata, so no output file is generated.
