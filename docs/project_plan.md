# AMC Dashboard - Project Plan

## Overview

The AMC Dashboard is a data-driven analytics platform designed to monitor, analyze, and forecast the performance of Indian Asset Management Companies (AMCs).

The platform will aggregate data from multiple sources, calculate key industry and AMC-level metrics, estimate current revenue, and forecast future revenue based on Assets Under Management (AUM), fund flows, market movements, and fee yields.

The dashboard is intended to be a read-only analytical platform with daily data refreshes and automated metric generation.

---

# Objectives

## Primary Objectives

* Track AMC-level AUM across the Indian mutual fund industry.
* Monitor fund-level and AMC-level inflows and outflows.
* Calculate market share and market share changes.
* Estimate AMC revenue using publicly available information.
* Forecast quarterly and annual revenue.
* Provide a centralized dashboard for management and research teams.

## Secondary Objectives

* Track category-level trends.
* Identify leading and lagging AMCs.
* Monitor expense ratio trends.
* Analyze flow attribution.
* Support future predictive analytics and scenario modeling.

---

# Business Questions

The platform should be capable of answering:

### Industry Level

* What is the total mutual fund industry AUM?
* Which categories are receiving the highest inflows?
* Which categories are experiencing the highest outflows?

### AMC Level

* Which AMCs are gaining market share?
* Which AMCs are losing market share?
* What is the current AUM of each AMC?
* What are the estimated revenues of each AMC?

### Fund Level

* Which schemes are attracting the highest inflows?
* Which schemes are seeing significant redemptions?
* How have expense ratios changed over time?

### Forecasting

* What is the expected revenue next quarter?
* What is the expected revenue for the next fiscal year?
* How sensitive is revenue to market movements and flows?

---

# System Architecture

```text
Data Sources
      │
      ▼
Data Ingestion
      │
      ▼
Data Warehouse
      │
      ▼
Analytics Engine
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

# Project Phases

## Phase 1 - Data Acquisition

### Goal

Collect and standardize all required datasets.

### Deliverables

* Fund master data
* Daily NAV data
* AUM data
* Expense ratio data
* AMC financial data

### Outputs

Raw datasets stored in the repository data layer.

---

## Phase 2 - Data Warehouse

### Goal

Create a centralized source of truth.

### Deliverables

Core warehouse tables:

* fund_master
* daily_nav
* daily_aum
* expense_ratio
* amc_financials

### Outputs

Clean, queryable datasets.

---

## Phase 3 - Analytics Engine

### Goal

Generate business metrics.

### Deliverables

* Net inflow calculations
* Net outflow calculations
* AUM growth calculations
* Category analytics
* AMC rankings

### Outputs

Analytics-ready datasets.

---

## Phase 4 - Market Share Engine

### Goal

Measure industry positioning.

### Deliverables

* AMC market share
* Market share changes
* Industry concentration metrics

### Outputs

Daily market share calculations.

---

## Phase 5 - Revenue Model

### Goal

Estimate AMC revenue.

### Methodology

Revenue will be estimated using:

Revenue ≈ Average AUM × Effective Yield

Where:

Effective Yield = Reported Revenue / Average AUM

### Deliverables

* Yield calculations
* Revenue estimates
* Historical validation

### Outputs

Daily revenue estimates.

---

## Phase 6 - Forecasting Engine

### Goal

Forecast future revenue and AUM.

### Forecast Components

* Historical flow trends
* Market performance assumptions
* Yield assumptions

### Scenarios

* Bull Case
* Base Case
* Bear Case

### Outputs

Quarterly and annual forecasts.

---

## Phase 7 - Dashboard

### Goal

Provide an intuitive interface for stakeholders.

### Dashboard Sections

#### Overview

* Industry AUM
* Industry Flows
* Industry Growth

#### AMC Analytics

* AUM
* Market Share
* Revenue Estimate

#### Fund Analytics

* Scheme Rankings
* Flow Rankings
* Expense Ratios

#### Forecasting

* Revenue Forecasts
* AUM Forecasts
* Scenario Analysis

---

## Phase 8 - Automation

### Goal

Automate data refresh and dashboard updates.

### Future Scope

* Daily ingestion jobs
* Automated calculations
* Automated exports
* Dashboard deployment updates

---

# Key Metrics

## Industry Metrics

* Total Industry AUM
* Industry Net Flows
* Industry Growth Rate

## AMC Metrics

* AUM
* Net Flows
* Market Share
* Revenue Estimate
* Revenue Growth

## Fund Metrics

* AUM
* Expense Ratio
* Net Flow
* Category Rank

## Forecast Metrics

* Forecast AUM
* Forecast Revenue
* Growth Scenarios

---

# Data Quality Requirements

The platform must validate:

* Missing values
* Duplicate records
* Inconsistent fund identifiers
* Invalid AUM values
* Invalid flow calculations

All transformations should be reproducible and auditable.

---

# Success Criteria

The project will be considered successful when it can:

1. Track all major Indian AMCs.
2. Calculate daily analytics without manual intervention.
3. Estimate AMC revenue within an acceptable error range versus reported financials.
4. Generate quarterly revenue forecasts.
5. Deliver a reliable read-only dashboard for internal stakeholders.

---

# Future Enhancements

Potential future capabilities include:

* Flow attribution analysis
* AMC peer comparison
* Category forecasting
* Machine learning forecasts
* Alerting and anomaly detection
* PDF report generation
* Executive summary generation

---
