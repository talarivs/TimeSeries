Overview:

- Extracts key fields dynamically using flexible JSON structures
- Computes hash-based time bucket keys from user-defined dimensions (like `productId`, `region`, `year`, etc.)
- Automatically creates hypertables using TimescaleDB in PostgreSQL for efficient time-series data storage
- Supports inserting data into **three independently managed table**:

- Adds SHA-256 hash indexes for fast lookup on `time_bucket_key`
- Stores all dynamic attributes and hashkey metadata in JSONB format

## Sample Input

```json
{
  "entity": "product_metrics",
  "entityType": "timeSeries",
  "timeBucketKey": ["productId", "region"],
  "timeBucket": ["quarterly", "annually"],
  "timeBucketInput": ["time_bucket", "year"],
  "productId": "P1234",
  "region": "Asia",
  "year": "2025",
  "time_bucket": "Q1",
  "attributes": [
    {
      "attributeName": "attr1",
      "type": "array",
      "attributes": [
        {
          "name": "",
          "weight": "",
          "score_value": "",
          "score": ""
        }
      ]
    },
    {
      "attributeName": "attr2",
      "type": "object",
      "attributes": {
        "name": "",
        "weight": "",
        "score_value": "",
        "score": ""
      }
    },
    {
      "attributeName": "attr3",
      "type": "string",
      "attributes": [""]
    }
  ]
}
```

## Database Structure (PostgreSQL + TimescaleDB)

### Prerequisites (Run once)

Enable TimescaleDB extension:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;
```

### Common Schema (all 3 tables)

| Column            | Type         | Description                                           |
|-------------------|--------------|-------------------------------------------------------|
| `time`            | TIMESTAMPTZ  | Timestamp of insertion                                |
| `hashkey_metadata`| JSONB        | Human-readable key string metadata                    |
| `time_bucket_key` | TEXT         | Concatenated hash input string (`productId:region:...`) |
| `attribute_list`  | JSONB        | Dynamic input attributes                              |

### Indexes

Each table uses a hash index for fast lookups on `time_bucket_key`:

```sql
CREATE INDEX IF NOT EXISTS idx_time_bucket_key_hash
ON product_metrics USING HASH (digest(time_bucket_key, 'sha256'));
```

###  Hypertables (TimescaleDB)

Each table is converted to a hypertable for time-series efficiency:

```sql
SELECT create_hypertable('product_metrics', 'time', if_not_exists => TRUE);
```

##  How It Works

### 1. Input Parsing
- Reads JSON input dynamically.
- Uses `timeBucketKey` and `timeBucket` to build a unique key.

### 2. Hashkey Generation
- Example:
  - Raw Key: `P1234:Asia:Q1:2025`
  - Metadata Key: `"productId_region_quarterly_year": "P1234AsiaQ12025"`

### 3. Date Resolution
- If full date not given:
  - `Q1` → `01`, `Q2` → `04`, `Q3` → `07`, `Q4` → `10`
  - Fallback to `"YYYY-01-01"` if nothing provided

### 4. Timescale Integration
- Automatically converts each table into a Timescale hypertable
- Supports scalable inserts and time-range queries

##  Tables Managed

- `product_metrics`


All are dynamically created with hypertable logic, indexes, and flexible schemas.

##  Validation Logic

Before processing:
- Required fields: `entity`, `entityType`, `attributes`, `timeBucketKey`, `timeBucket`
- Each attribute must contain:
  - `"attributeName"` and `"type"`
- Ensures no missing keys or malformed structure

## Setup & Run

1. Start PostgreSQL (ensure TimescaleDB is enabled).
2. Clone the project and configure `application.properties`.
3. Run the Spring Boot app.
