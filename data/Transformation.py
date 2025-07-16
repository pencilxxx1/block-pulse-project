import pandas as pd

# Data Transformation
def run_transformation():
    data = pd.read_json(r'MarketsData.json')

    # Remove Duplicates
    data.drop_duplicates(inplace=True)

    # Handling missing values ( filling missing numeric values with mean or median )
    numeric_columns = data.select_dtypes(include=['float64', 'Int64']).columns
    for col in numeric_columns:
        data.fillna({col: data[col].mean()}, inplace=True)
    
    # Handling missing values ( filling missing object/string values with 'Unknown' )
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col: 'Unknown'}, inplace=True)

    # creating fact and dimension tables
    # creating a crypto_assets table
    crypto_assets = data[['name', 'symbol', 'current_price', 'market_cap', 'total_volume', 'price_change_percentage_24h']].drop_duplicates().reset_index(drop=True)
    crypto_assets.index.name = 'asset_id'
    crypto_assets = crypto_assets.reset_index()

    # create crypto_market table
    crypto_market = data[['market_cap_rank', 'high_24h', 'low_24h', 'price_change_24h', 'price_change_percentage_24h']].drop_duplicates().reset_index(drop=True)
    crypto_market.index.name = 'market_id'
    crypto_market = crypto_market.reset_index()

    # create crypto_prices table
    crypto_prices = data[['current_price', 'price_change_24h', 'price_change_percentage_24h']].drop_duplicates().reset_index(drop=True)
    crypto_prices.index.name = 'price_id'
    crypto_prices = crypto_prices.reset_index()

    # create crypto datetime table
    crypto_datetime = data[['name', 'last_updated']].drop_duplicates().reset_index(drop=True)
    crypto_datetime.index.name = 'datetime_id'
    crypto_datetime = crypto_datetime.reset_index()

    # transaction table
    transaction = data.merge(crypto_assets, on=['name', 'symbol', 'current_price', 'market_cap', 'total_volume', 'price_change_percentage_24h'], how='left') \
                    .merge(crypto_market, on=['market_cap_rank', 'high_24h', 'low_24h', 'price_change_24h', 'price_change_percentage_24h'], how='left') \
                    .merge(crypto_prices, on=['current_price', 'price_change_24h', 'price_change_percentage_24h'], how='left') \
                    .merge(crypto_datetime, on=['name', 'last_updated'], how='left')

    transaction.index.name = 'transaction_id'
    transaction = transaction.reset_index() \
                            [[ 'transaction_id', 'asset_id', 'market_id', 'price_id', 'datetime_id', 
                            'name', 'symbol', 'current_price', 'market_cap', 'total_volume', 
                            'price_change_percentage_24h', 'market_cap_rank', 'high_24h', 
                            'low_24h', 'price_change_24h', 'last_updated']]

    # Saving the data to CSV files
    data.to_csv('cleaned_data.csv', index=False)
    crypto_assets.to_csv('crypto_assets.csv', index=False)
    crypto_market.to_csv('crypto_market.csv', index=False)
    crypto_prices.to_csv('crypto_prices.csv', index=False)
    crypto_datetime.to_csv('crypto_datetime.csv', index=False)
    transaction.to_csv('transaction.csv', index=False)

    print('Data cleaning and transformation completed successfully')