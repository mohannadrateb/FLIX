import pandas as pd

costs = {
    "SEGMENTS_DATE": ["2024-01-01", "2024-01-02"],
    "CUSTOMER_ID": [123, 124],
    "CAMPAIGN_ID": [1, 2],
    "METRICS_COST_MICROS": [1000000, 2000000],
    "LAST_EXTRACTED_AT": ["2024-01-02T10:00:00Z", "2024-01-02T11:00:00Z"]
}
df = pd.DataFrame(costs)
df.to_csv("test_data/raw_costs_test.csv", index=False)



# Define the test data
account = {
    "ACCOUNT_ID": [123, 124],
    "CURRENCY": ["EUR", "USD"],
    "CHANNEL_TYPE": ["Online", "Offline"],
    "AD_PLATFORM": ["Google Ads", "Facebook Ads"],
    "BRAND": ["BrandA", "BrandB"],
    "CAMPAIGN_OBJECTIVE": ["Awareness", "Conversions"],
    "DOMAIN": ["brand-a.com", "brand-b.com"],
    "LANGUAGE": ["EN", "DE"]
}

# Create DataFrame
accounts_info_df = pd.DataFrame(account)

# Save to CSV
accounts_info_df.to_csv("test_data/accounts_info_test.csv", index=False)


exchange_rates = {
    "CURRENCY": [ "USD"],
    "BASE": [ "EUR"],
    "RATE": [ 1.2],
    "VALID_FROM": ["2024-01-01"],
    "VALID_TO": ["2024-01-31"]
}
exchange_rates_df = pd.DataFrame(exchange_rates)
exchange_rates_df.to_csv("test_data/exchange_rates_test.csv", index=False)
    



