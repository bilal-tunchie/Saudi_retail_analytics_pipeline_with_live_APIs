# Saudi Retail & E-Commerce Data Pipeline

> **End-to-end data engineering portfolio project** — A production-grade Medallion architecture pipeline ingesting live Saudi retail data from multiple APIs, transforming through Bronze → Silver → Gold layers, and serving a star schema analytics model in Power BI.

<br>

## Project Overview

This project simulates a real-world retail analytics platform for the Saudi e-commerce market. Built entirely from scratch as a solo data engineering effort, it demonstrates the full spectrum of modern data engineering skills — from API ingestion and distributed processing to dimensional modeling and business intelligence.

| Attribute | Detail |
|---|---|
| **Market** | Saudi Arabia 🇸🇦 |
| **Data Volume** | ~12,000+ records across 4 source systems |
| **Date Range** | January 2024 – December 2026 |
| **Architecture** | Medallion (Bronze → Silver → Gold) |
| **Processing** | Apache Spark / PySpark on Databricks |
| **Storage** | Delta Lake |
| **Modeling** | Star Schema (2 Facts, 7 Dimensions) |
| **Visualization** | Microsoft Power BI (Direct Query) |
| **Version Control** | Git / GitHub |

<br>

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
│                                                                   │
│  HasData Amazon.sa API    Mockaroo (Orders)    Open-Meteo         │
│  ─────────────────────    ────────────────     ──────────         │
│  Products · Prices        Synthetic Orders     Weather History    │
│  Ratings · Badges         6,000 records        5,310 records      │
│                                                                   │
│                        Calendarific API                           │
│                        ────────────────                           │
│                        Saudi Holidays · 3 Years                  │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    🥉 BRONZE LAYER                                │
│              Raw ingestion — immutable, append-only              │
│                                                                   │
│   bronze_product    bronze_orders    weather    bronze_holidays   │
│   bronze_orders_enriched (product injection)                     │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                    🥈 SILVER LAYER                                │
│         Cleaned · Type-cast · Normalized · Deduplicated          │
│                                                                   │
│   silver_product    silver_orders    silver_weather              │
│   silver_holidays                                                 │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                     🥇 GOLD LAYER                                 │
│              Star Schema · Business Logic · Aggregations         │
│                                                                   │
│   fact_orders        fact_weather                                │
│   dim_product        dim_customer      dim_city                  │
│   dim_payment_method dim_device_type   dim_holidays              │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               ▼
                    ┌─────────────────┐
                    │   Power BI      │
                    │  Direct Query   │
                    │  Star Schema    │
                    └─────────────────┘
```

<br>

---

## 🗂️ Repository Structure

```
Saudi_retail_pipeline/
│
├── scripts/
│   ├── 00_config.ipynb              # Global config · catalog · constants · API keys
│   │
│   ├── bronze/
│   │   ├── 02_bronze_products.ipynb       # HasData Amazon.sa scraper
│   │   ├── 03_bronze_orders_enrich.ipynb  # Mockaroo orders + product injection
│   │   ├── 04_bronze_holidays.ipynb       # Calendarific Saudi holidays
│   │   └── 05_bronze_weather.ipynb        # Open-Meteo historical weather
│   │
│   ├── silver/
│   │   ├── 07_silver_products.ipynb       # Type casting · dedup · category normalization
│   │   ├── 08_silver_orders.ipynb         # Date parsing · city standardization
│   │   ├── 09_silver_weather.ipynb        # Numeric casting · dedup by city+date
│   │   └── 10_silver_holidays.ipynb       # Date cleaning · ISO country codes
│   │
│   └── gold/
│       ├── 11_gold_dim_holiday.ipynb      # Saudi holiday dimension
│       ├── 12_gold_dim_product.ipynb      # Product dimension + discount logic
│       ├── 13_gold_dim_city.ipynb         # City dimension + coordinates
│       ├── 14_gold_dim_customer.ipynb     # Customer dimension + surrogate keys
│       ├── 15_gold_dim_payment_method.ipynb
│       ├── 16_gold_dim_device_type.ipynb
│       ├── 17_gold_fact_weather.ipynb     # Weather fact table
│       └── 18_gold_fact_orders.ipynb      # Orders fact table + all FK joins
│
└── README.md
```

<br>

---

## 🌟 Star Schema

```
                        dim_product
                        (asin PK)
                             │
          dim_customer        │         dim_city
          (customer_id PK)   │         (city_id PK)
                    │         │              │
                    └────┐    │    ┌─────────┘
                         ▼    ▼    ▼
dim_payment_method ──► fact_orders ◄── dim_device_type
(payment_method_id PK)  (order_id)     (device_type_id PK)
                             │
                        order_date
                             │
                         dim_date ──► dim_holidays
                         (Date PK)    (holiday_date FK)
                             │
                        fact_weather
                        (date + city_id)
                             │
                          dim_city
                          (city_id PK)
```

<br>

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| **Compute** | Databricks Community Edition |
| **Language** | Python 3.12 · PySpark · SQL |
| **Storage** | Delta Lake (ACID transactions) |
| **Catalog** | Unity Catalog |
| **APIs** | HasData · Open-Meteo · Calendarific · Mockaroo |
| **BI Layer** | Power BI Desktop (Direct Query) |
| **Version Control** | Git · GitHub |

<br>

---

## 🔑 Key Engineering Decisions

### 1. Immutable Bronze Layer
Bronze tables are **append-only and never modified**. All enrichment and cleaning happens in dedicated downstream tables. This preserves raw data integrity and enables full pipeline replayability.

### 2. Deduplication Strategy
Weather data is deduplicated at two levels:
- **Python level** — within each API batch using a `seen` set before DataFrame creation
- **Spark level** — `left_anti` join against existing Bronze table before append, ensuring no city+date duplicates regardless of how many times the pipeline runs

### 3. Mixed Date Format Handling
Bronze weather had inconsistent `_snapshot_date` formats (`yyyy-MM-dd` vs `dd-MM-yyyy`) due to schema evolution across pipeline runs. Resolved by standardizing all Bronze records to `dd-MM-yyyy` using a CASE expression, then converting to native `DateType` in Silver.

### 4. Dynamic Category Normalization
Instead of hardcoding category mappings, Silver products derives categories dynamically from Bronze data at runtime — replacing underscores, applying `.title()` casing. Adding a new category to the API requires zero code changes.

### 5. Surrogate Key Design
Surrogate keys use meaningful prefixes rather than plain integers:
- `NACA100001` — customers (NACA = Saudi nationality code reference)
- `CITY_RIYADH` — cities (human-readable, stable)
- `PAMA100001` — payment methods
- `DATA100001` — device types

### 6. Gold Layer Business Logic
All business logic — discount calculations, price change percentages, foreign key resolution — lives exclusively in Gold. Silver is a clean, business-agnostic representation of source data.

<br>

---

## 📊 Data Sources

| Source | API | Records | Refresh |
|---|---|---|---|
| Amazon.sa Products | HasData | ~542 products · 10 categories | Manual (CE limitation) |
| Orders | Mockaroo | 6,000 synthetic orders | On-demand |
| Weather | Open-Meteo Archive | 5,310 rows · 6 cities · 2 years | Daily append |
| Holidays | Calendarific | 52 holidays · 3 years | Annual |

> **Note on Databricks CE:** Community Edition restricts outbound internet to certain domains. Open-Meteo is whitelisted and runs directly from the cluster. Other APIs are fetched externally via Postman/VS Code and loaded via DBFS — a pattern that mirrors real enterprise ingestion patterns where API calls are separated from transformation workloads.

<br>

---

## 📐 Gold Layer Tables

### Fact Tables

| Table | Rows | Grain | Key Measures |
|---|---|---|---|
| `fact_orders` | 6,000 | One row per order | quantity · unit_price_sar · total_amount |
| `fact_weather` | 5,310 | One row per city per day | temperature · precipitation · windspeed |

### Dimension Tables

| Table | Rows | Natural Key | Surrogate Key |
|---|---|---|---|
| `dim_product` | 542 | asin | — |
| `dim_customer` | 6,000 | full_name | NACA1XXXXX |
| `dim_city` | 6 | city | CITY_XXXXX |
| `dim_payment_method` | 7 | payment_method | PAMA1XXXXX |
| `dim_device_type` | 4 | device_type | DATA1XXXXX |
| `dim_holiday` | 52 | holiday_date | — |

<br>

---

## 📈 Dashboard Pages (Power BI)

| Page | Business Questions Answered |
|---|---|
| **Executive Overview** | Total revenue · order trends · city performance · status breakdown |
| **Product Performance** | Category revenue · top products · discount impact · price distribution |
| **Customer Behavior** | Device preferences · payment methods · ordering patterns by day/month |
| **Weather & Sales** | Temperature vs order volume · precipitation impact · weather on peak days |
| **Holiday Impact** | National holiday revenue uplift · Ramadan patterns · Eid sales spikes |
| **Operations** | Cancellation rates · return rates · city-level operational metrics |

<br>

---

## 🚀 How to Run

### Prerequisites
- Databricks workspace (Community Edition or higher)
- Python 3.10+
- API keys: HasData · Calendarific

### Setup

**1. Clone the repository**
```bash
git clone https://github.com/bilalmohammed_tunchie/Saudi_retail_pipeline.git
```

**2. Import notebooks to Databricks**
```
Workspace → Import → upload all .ipynb files maintaining folder structure
```

**3. Configure 00_config**
```python
CATALOG            = "saudi_retail_catalog"
HASDATA_KEY        = "your_key_here"
CALENDARIFIC_KEY   = "your_key_here"
```

**4. Run in order**
```
00_config → Bronze (02–05) → Silver (07–10) → Gold (11–18)
```

<br>

---

## 🧠 What This Project Demonstrates

- **API integration** across 4 heterogeneous sources with different authentication, response formats, and reliability characteristics
- **Delta Lake** expertise — ACID transactions, schema evolution, time travel awareness, merge patterns
- **PySpark** at scale — window functions, complex joins, dynamic expressions, broadcast optimization
- **Medallion architecture** — strict layer separation with clear contracts between Bronze/Silver/Gold
- **Dimensional modeling** — star schema design, surrogate key strategy, SCD awareness
- **Data quality** — deduplication at multiple levels, null handling, format normalization, referential integrity validation
- **Production thinking** — idempotent pipelines, graceful error handling, config-driven design with no hardcoded values

<br>

---

## 👤 Author

**Bilal Mohammed**
CRM Designer → Data Engineer
Samsung Electronics · Riyadh, Saudi Arabia

[![GitHub](https://img.shields.io/badge/GitHub-bilal--tunchie-181717?style=flat&logo=github)](https://github.com/bilalmohammed_tunchie)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin)](https://linkedin.com/in/bilalmohammed)

<br>

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

> *Built with PySpark, Delta Lake, and too much Saudi coffee ☕*
