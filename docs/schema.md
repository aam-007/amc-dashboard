# Warehouse Schema

## Purpose

This document defines the canonical data model for the AMC Dashboard platform.

All analytics, revenue calculations, forecasts, and dashboard visualizations must be derived from these tables.

The warehouse is designed around a star-schema approach:

```text
Fund Master
      │
      ├── Daily NAV
      ├── Daily AUM
      ├── Expense Ratio
      └── Flow Metrics

AMC Financials
      │
      └── Revenue Model

Analytics Layer
      │
      └── Dashboard
```

---

# Table: fund_master

## Purpose

Master reference table containing scheme metadata.

## Primary Key

```text
fund_id
```

## Columns

| Column      | Type    |
| ----------- | ------- |
| fund_id     | STRING  |
| scheme_name | STRING  |
| amc_name    | STRING  |
| category    | STRING  |
| subcategory | STRING  |
| plan_type   | STRING  |
| benchmark   | STRING  |
| launch_date | DATE    |
| is_active   | BOOLEAN |

## Example

```text
fund_id: HDFC_SCF_DIRECT
scheme_name: HDFC Small Cap Fund
amc_name: HDFC AMC
category: Equity
subcategory: Small Cap
plan_type: Direct
```

---

# Table: daily_nav

## Purpose

Stores historical NAV values.

## Grain

One row per:

```text
fund_id
date
```

## Primary Key

```text
(fund_id, date)
```

## Columns

| Column  | Type   |
| ------- | ------ |
| date    | DATE   |
| fund_id | STRING |
| nav     | FLOAT  |

---

# Table: daily_aum

## Purpose

Stores historical Assets Under Management.

## Grain

One row per:

```text
fund_id
date
```

## Primary Key

```text
(fund_id, date)
```

## Columns

| Column  | Type   |
| ------- | ------ |
| date    | DATE   |
| fund_id | STRING |
| aum_cr  | FLOAT  |

---

# Table: expense_ratio

## Purpose

Tracks historical expense ratios.

## Grain

One row per:

```text
fund_id
effective_date
```

## Primary Key

```text
(fund_id, effective_date)
```

## Columns

| Column         | Type   |
| -------------- | ------ |
| effective_date | DATE   |
| fund_id        | STRING |
| expense_ratio  | FLOAT  |

---

# Table: amc_financials

## Purpose

Stores reported AMC financial results.

## Grain

One row per:

```text
amc
quarter
```

## Primary Key

```text
(amc_name, quarter)
```

## Columns

| Column         | Type   |
| -------------- | ------ |
| quarter        | STRING |
| amc_name       | STRING |
| revenue_cr     | FLOAT  |
| profit_cr      | FLOAT  |
| average_aum_cr | FLOAT  |

---

# Table: fund_flows

## Purpose

Stores calculated fund-level flow metrics.

## Grain

One row per:

```text
fund_id
date
```

## Primary Key

```text
(fund_id, date)
```

## Columns

| Column      | Type   |
| ----------- | ------ |
| date        | DATE   |
| fund_id     | STRING |
| inflow_cr   | FLOAT  |
| outflow_cr  | FLOAT  |
| net_flow_cr | FLOAT  |

## Notes

Initially:

```text
inflow_cr = NULL
outflow_cr = NULL

net_flow_cr = estimated value
```

until a more precise methodology is implemented.

---

# Table: amc_daily_metrics

## Purpose

Daily AMC-level aggregation table.

This table powers most dashboard views.

## Grain

One row per:

```text
amc_name
date
```

## Primary Key

```text
(amc_name, date)
```

## Columns

| Column               | Type   |
| -------------------- | ------ |
| date                 | DATE   |
| amc_name             | STRING |
| total_aum_cr         | FLOAT  |
| net_flow_cr          | FLOAT  |
| market_share_pct     | FLOAT  |
| estimated_revenue_cr | FLOAT  |

---

# Table: industry_daily_metrics

## Purpose

Industry-wide aggregates.

## Grain

One row per:

```text
date
```

## Primary Key

```text
date
```

## Columns

| Column               | Type    |
| -------------------- | ------- |
| date                 | DATE    |
| industry_aum_cr      | FLOAT   |
| industry_net_flow_cr | FLOAT   |
| active_funds         | INTEGER |
| active_amcs          | INTEGER |

---

# Table: revenue_estimates

## Purpose

Stores calculated AMC revenue estimates.

## Grain

One row per:

```text
amc_name
date
```

## Primary Key

```text
(amc_name, date)
```

## Columns

| Column               | Type   |
| -------------------- | ------ |
| date                 | DATE   |
| amc_name             | STRING |
| estimated_revenue_cr | FLOAT  |
| effective_yield_pct  | FLOAT  |

---

# Table: revenue_forecasts

## Purpose

Stores forecast outputs.

## Grain

One row per:

```text
amc_name
forecast_date
scenario
```

## Primary Key

```text
(amc_name, forecast_date, scenario)
```

## Columns

| Column              | Type   |
| ------------------- | ------ |
| forecast_date       | DATE   |
| amc_name            | STRING |
| scenario            | STRING |
| forecast_aum_cr     | FLOAT  |
| forecast_revenue_cr | FLOAT  |

---

# Dashboard Data Sources

## Overview Page

```text
industry_daily_metrics
amc_daily_metrics
```

## AMC Detail Page

```text
fund_master
daily_aum
fund_flows
revenue_estimates
```

## Forecast Page

```text
revenue_forecasts
```

---

# MVP Tables

The minimum viable platform requires only:

```text
fund_master
daily_nav
daily_aum
amc_financials
amc_daily_metrics
```

All other tables can be derived later.

--dated: June 2026
