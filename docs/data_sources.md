# Data Sources

## Purpose

This document serves as the master inventory of all datasets required by the AMC Dashboard project.

Each dataset is classified by:

* Business purpose
* Source
* Refresh frequency
* Ingestion status
* Priority level

---

# Data Source Categories

The platform relies on five primary data domains:

1. Fund Master Data
2. NAV & AUM Data
3. Flow Data
4. Expense Ratio Data
5. AMC Financial Data

---

# 1. Fund Master Data

## Purpose

Provides the reference mapping between mutual fund schemes and their corresponding Asset Management Companies (AMCs).

Without this dataset, AMC-level aggregation is not possible.

## Required Fields

| Field       | Description              |
| ----------- | ------------------------ |
| fund_id     | Internal fund identifier |
| scheme_name | Scheme name              |
| amc_name    | AMC name                 |
| category    | Fund category            |
| subcategory | Fund subcategory         |
| plan_type   | Direct / Regular         |
| launch_date | Scheme launch date       |

## Usage

Used by:

* Market Share Engine
* Revenue Model
* Forecasting Engine

## Priority

Critical

## Status

Not Yet Implemented

---

# 2. NAV Data

## Purpose

Tracks scheme Net Asset Values over time.

Used to measure market performance and estimate market-driven AUM growth.

## Required Fields

| Field       | Description |
| ----------- | ----------- |
| date        | NAV date    |
| scheme_name | Scheme name |
| nav         | NAV value   |

## Source

AMFI

## Refresh Frequency

Daily

## Usage

* Market impact calculations
* Historical performance analysis
* Revenue forecasting

## Priority

Critical

## Status

Available via AMFI ingestion pipeline

---

# 3. AUM Data

## Purpose

Tracks Assets Under Management for every scheme.

AUM is the core driver of AMC revenue.

## Required Fields

| Field       | Description      |
| ----------- | ---------------- |
| date        | Observation date |
| scheme_name | Scheme name      |
| aum_cr      | AUM in crore INR |

## Source

AMFI

## Refresh Frequency

Daily / Monthly

## Usage

* Revenue estimation
* Market share calculation
* Flow calculations
* Forecasting

## Priority

Critical

## Status

Available via AMFI ingestion pipeline

---

# 4. Flow Data

## Purpose

Measures investor subscriptions and redemptions.

Flow data is one of the most important metrics for AMC performance analysis.

## Required Fields

| Field       | Description            |
| ----------- | ---------------------- |
| date        | Observation date       |
| scheme_name | Scheme name            |
| inflow      | Investor subscriptions |
| outflow     | Investor redemptions   |
| net_flow    | Net flow               |

## Source

Derived

## Calculation

Net Flow ≈

Current AUM

− Prior AUM

− Market Performance Effect

## Usage

* AMC rankings
* Market share analysis
* Revenue forecasting

## Priority

Critical

## Status

Planned

---

# 5. Expense Ratio Data

## Purpose

Tracks fees charged by mutual fund schemes.

Expense ratios influence AMC economics and profitability.

## Required Fields

| Field         | Description    |
| ------------- | -------------- |
| date          | Effective date |
| scheme_name   | Scheme name    |
| expense_ratio | Expense ratio  |

## Source

Fund Factsheets

## Refresh Frequency

Monthly

## Usage

* Revenue model
* Yield analysis
* Competitive analysis

## Priority

High

## Status

Not Yet Implemented

---

# 6. AMC Financial Data

## Purpose

Provides actual reported financial performance for AMCs.

Used to derive effective yield and validate revenue estimates.

## Required Fields

| Field       | Description           |
| ----------- | --------------------- |
| quarter     | Reporting quarter     |
| amc_name    | AMC name              |
| revenue     | Revenue               |
| profit      | Net profit            |
| average_aum | Average AUM           |
| yield       | Revenue ÷ Average AUM |

## Sources

* Annual Reports
* Investor Presentations
* Earnings Releases

## Refresh Frequency

Quarterly

## Usage

* Revenue model calibration
* Revenue validation
* Forecasting

## Priority

Critical

## Status

Not Yet Implemented

---

# 7. Industry Benchmark Data

## Purpose

Tracks aggregate mutual fund industry metrics.

Required for market share calculations.

## Required Fields

| Field        | Description        |
| ------------ | ------------------ |
| date         | Observation date   |
| industry_aum | Total industry AUM |

## Source

AMFI

## Refresh Frequency

Monthly

## Usage

* Market share calculations
* Industry trend analysis

## Priority

High

## Status

Planned

---

# Data Pipeline Flow

```text
Fund Master
        │
        ▼
NAV + AUM
        │
        ▼
Flow Calculations
        │
        ▼
AMC Aggregation
        │
        ▼
Market Share Engine
        │
        ▼
Revenue Model
        │
        ▼
Forecast Engine
        │
        ▼
Dashboard
```

---

# Source Priority Matrix

| Dataset             | Priority |
| ------------------- | -------- |
| Fund Master         | Critical |
| NAV Data            | Critical |
| AUM Data            | Critical |
| Flow Data           | Critical |
| AMC Financials      | Critical |
| Expense Ratio Data  | High     |
| Industry Benchmarks | High     |

---

# Immediate Next Steps

1. Verify whether AMFI exposes AMC name for every scheme.
2. Build Fund Master dataset.
3. Integrate NAV ingestion into warehouse.
4. Integrate AUM ingestion into warehouse.
5. Design warehouse schema.
6. Implement flow calculation engine.

---
