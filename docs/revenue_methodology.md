# Revenue Estimation Framework

## What Is Being Estimated

The model estimates management fee revenue generated from mutual fund assets under management.

It does not attempt to estimate total AMC revenue.

AMC financial statements may include additional income sources such as:

* Exit load retention
* Transaction and service charges
* Treasury income
* Investment income on sponsor capital
* Other operating income

Accordingly, throughout this document:

Revenue = Estimated Management Fee Revenue

unless otherwise stated.

---

# Monthly Revenue Estimation

Monthly revenue should be estimated using monthly average AUM rather than by simply dividing annual revenue by twelve.

Preferred Formula:

Monthly Revenue =
Monthly Average AUM × Effective Yield / 12

This approach captures changes in AUM throughout the year and provides a more realistic estimate of fee generation.

Monthly revenue estimates are used for:

* Trend analysis
* Growth tracking
* Forecasting
* Dashboard visualizations

---

# Category-Based Yield Model

Different fund categories generate different fee levels.

Illustrative hierarchy:

| Category                | Relative Yield |
| ----------------------- | -------------- |
| Equity Funds            | Highest        |
| Hybrid Funds            | High           |
| Solution-Oriented Funds | High to Medium |
| Debt Funds              | Medium         |
| Index Funds             | Low            |
| ETFs                    | Lowest         |
| Liquid Funds            | Very Low       |

The table is intended only as a directional representation.

Actual yield assumptions used in the model are maintained separately and may vary by:

* Scheme category
* Direct vs Regular plans
* AMC-specific pricing strategies
* Regulatory changes

Revenue estimates should therefore rely on documented numerical yield assumptions rather than category ordering alone.

---

# Yield Assumptions

Category-level yield assumptions are maintained separately from the methodology document.

Example structure:

| Category | Assumed Yield |
| -------- | ------------- |
| Equity   | 0.95%         |
| Hybrid   | 0.80%         |
| Debt     | 0.45%         |
| Index    | 0.20%         |
| ETF      | 0.10%         |
| Liquid   | 0.15%         |

These values are illustrative only.

Actual assumptions will be calibrated using:

* Expense ratio disclosures
* AMC annual reports
* Investor presentations
* Historical validation exercises

---

# Forecast Revenue Methodology

Future management fee revenue is determined by two components:

Revenue Forecast =
Forecast AUM × Forecast Yield

## Forecast AUM

Forecast AUM incorporates:

* Net investor flows
* Market performance
* Category allocation changes
* Industry growth trends

These factors collectively determine future asset levels.

## Forecast Yield

Forecast yield represents the effective fee realization rate earned on assets.

Yield assumptions may be modeled using one of three approaches:

### Flat Yield

Assumes no meaningful change in fee realization.

Used for:

* Short-term forecasts
* Stable market environments

### Linear Compression

Assumes gradual fee pressure over time.

Example:

* Year 1: 0.75%
* Year 2: 0.73%
* Year 3: 0.71%

Used when passive products continue gaining market share.

### Scenario-Based Yield

Different yield assumptions are assigned to each forecast scenario.

Example:

| Scenario | Yield Assumption   |
| -------- | ------------------ |
| Bull     | Stable             |
| Base     | Mild Compression   |
| Bear     | Strong Compression |

This approach allows the model to explicitly incorporate competitive pressure and industry fee trends.

---

# Core Formula

The foundational model used throughout the project is:

Management Fee Revenue =
AUM × Effective Yield

This formula estimates recurring fee income generated from managed assets.

It should not be interpreted as total reported AMC revenue, which may include additional income sources outside the scope of this model.
