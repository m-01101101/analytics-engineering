# checkout.com

This project is part of the Data Analytics Engineering interview process for checkout.com

## Task

Using the data provided in the tables `onboarding_applications`, `dim_user` `dim_opportunity`, `dim_merchant` and `dim_country` construct a data model in `dbt` that contains the following features for each application so that our data analysts can produce the final report in Looker:

- Application ID
- Application Created Month
- Current Status
- Risk Level
- Merchant Account ID
- Merchant Name
- Country Full Name
- Region
- Sales Rep Name
- Lead Source Bucket
- Final Reviewer Name
- Total Onboarded Merchants in a given Month
  - _not included in the dbt model as it represents a different level of aggregation_
- Hours to Initial Review
- Days to Onboard

## Considerations

1. `onboarding_applications` is an event-level data meaning that every change to an application is stored as a new record for that application.
2. A merchant is onboarded if the Merchant Account ID field is populated
3. Hours to initial review is the number of business hours during weekdays (weekends excluded) between created and the first in progress dates
4. Days to complete onboarding is the number of business days (weekends excluded) between created and first completed dates
5. merchant names are updated in dim_merchant. Historical data in the model should reflect the latest merchant name for a given merchant_id, not necessarily the one used at the time of the application.

## Outputs

1. materialise the result of your model as an incremental table.
2. `dbt` project (including a `dbt` test file).
