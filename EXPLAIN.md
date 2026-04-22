# Design and implementation notes

Document architecture, data flow, and modeling choices for this project here.

## Data Quality Handling – Company Metadata

The provided `company_metadata.json` contains inconsistencies between the `is_public` and `stock_ticker` fields. For example, some companies marked as private (`is_public: false`) have non-null stock tickers (e.g., OpenAI with ticker `"OPEN"`).

In this project, such cases are treated as **data quality issues** rather than valid business logic. The fields disagree: either the company is public and has a ticker, or it is private and has no ticker.

### Resolution Strategy

- A **non-null `stock_ticker`** is treated as evidence that the company is publicly traded.
- If `stock_ticker` is present and `is_public` is `false`, then:
  - **`is_public` is set to `true`** (the ticker is kept as given).
- If `is_public` is `false` and there is no ticker, no change is needed.
- These corrections can optionally be flagged for visibility (not required for downstream logic).
### On Category and Industry Joins

During ETL, the project never **joins on category and industry fields** between news and company metadata. These two fields are treated independently:

- The `category` in news data is mapped using a custom taxonomy (`src/etl/category_taxonomy.py`), standardizing a variety of publisher-provided tags (e.g., "AI & ML", "Cloud", "Fintech") to broad UPPER_SNAKE codes.
- The `industry` in company metadata comes directly from the metadata JSON and typically reflects a company's established sector or vertical, not the topical focus of an article.

**No join or relationship is established in the ETL pipeline based on these fields.** There is intentionally no mapping or cross-referencing step such as `df["category"] == meta["industry"]`, and no attempt is made to align categories/taxonomies across data sources for the following reasons:

- **Editorial independence:** News categories describe the topic of a particular article, while company industry reflects general business domain.
- **Non-overlapping definitions:** Companies can be mentioned in articles outside their primary sector, and news categories are usually much coarser or broader than even high-level industries.
- **No added insight from forced join:** Attempting to join or filter on equality between article category and company industry would arbitrarily exclude relevant records and add minimal, if any, analytic value.

If examining overlap—or mismatch—between news topics and company sectors is desired for analysis, this is best performed downstream, after ETL, for exploratory or statistical purposes only (see e.g., `tests/scratch.py`).

**Summary:**  
There is no join key or logic in the ETL pipeline that relates `category` to `industry`. Each serves a distinct, complementary purpose in the processed data.