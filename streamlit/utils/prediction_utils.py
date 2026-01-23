import pandas as pd
from datetime import datetime, timedelta
# -------------------- PRODUCT FORECAST --------------------
# -------------------- PRODUCT FORECAST --------------------
from prophet import Prophet

def predict_product_sales(df: pd.DataFrame, forecast_days: int = 30) -> pd.DataFrame:
    """
    Forecast daily sales per item using Prophet.
    
    Parameters:
    - df: DataFrame with columns ['item_name', 'order_date', 'qty']
    - forecast_days: number of future days to forecast
    
    Returns:
    - DataFrame with columns ['item_name', 'date', 'predicted_units']
    """

    # Ensure datetime
    df['order_date'] = pd.to_datetime(df['order_date'])

    forecast_list = []

    for item in df['item_name'].unique():
        # Filter data for the item
        item_df = df[df['item_name'] == item].groupby('order_date', as_index=False)['qty'].sum()
        item_df = item_df.rename(columns={'order_date': 'ds', 'qty': 'y'})

        # Check if enough data
        if len(item_df) < 5:
            # Too little data → fallback to average
            avg_qty = item_df['y'].mean() if not item_df.empty else 0
            today = pd.Timestamp.today()
            for i in range(forecast_days):
                forecast_list.append({
                    'item_name': item,
                    'date': today + pd.Timedelta(days=i+1),
                    'predicted_units': round(avg_qty, 1)
                })
            continue

        # Train Prophet model
        model = Prophet(daily_seasonality=True, yearly_seasonality=False, weekly_seasonality=True)
        model.fit(item_df)

        # Make future dataframe
        future = model.make_future_dataframe(periods=forecast_days)
        forecast = model.predict(future)

        # Extract forecast only for future dates
        future_forecast = forecast[['ds', 'yhat']].tail(forecast_days)
        for _, row in future_forecast.iterrows():
            forecast_list.append({
                'item_name': item,
                'date': row['ds'],
                'predicted_units': max(0, round(row['yhat'], 1))  # avoid negative predictions
            })

        # Optional: include historical data as well
        for _, row in item_df.iterrows():
            forecast_list.append({
                'item_name': item,
                'date': row['ds'],
                'predicted_units': row['y']
            })

    df_result = pd.DataFrame(forecast_list)
    df_result = df_result.sort_values(['date', 'item_name']).reset_index(drop=True)
    return df_result



# -------------------- CUSTOMER FORECAST --------------------
def predict_customers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Returns likely active customers using RFM-like rules (recency + frequency).
    """
    today = pd.Timestamp.today()
    customer_stats = df.groupby('customer_name').agg({
        'order_date': lambda x: (today - x.max()).days,
        'sales_order_id': 'nunique',
        'qty': 'sum'
    }).reset_index()
    customer_stats.rename(columns={
        'order_date': 'days_since_last_order',
        'sales_order_id': 'num_orders',
        'qty': 'total_qty'
    }, inplace=True)
    # Simple scoring: lower days_since_last_order + higher num_orders → more likely
    customer_stats['likelihood_score'] = (1 / (1 + customer_stats['days_since_last_order'])) * customer_stats['num_orders']
    customer_stats.sort_values('likelihood_score', ascending=False, inplace=True)
    return customer_stats[['customer_name', 'days_since_last_order', 'num_orders', 'total_qty', 'likelihood_score']]


# -------------------- CUSTOMER → PRODUCT PREDICTION --------------------
def customer_product_prediction(df: pd.DataFrame, forecast_days: int = 30) -> pd.DataFrame:
    """
    Predict top items per customer and spread predicted quantity evenly across next forecast_days.
    
    Returns:
        DataFrame with columns: customer_name, item_name, predicted_date, predicted_units
    """
    if 'qty' not in df.columns:
        raise ValueError("Column 'qty' not found in DataFrame")

    # Sum total qty per customer per item
    top_items = df.groupby(['customer_name', 'item_name'], as_index=False)['qty'].sum()

    forecast_list = []

    # Start from next day after last order date
    last_date = df['order_date'].max()
    future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=forecast_days, freq='D')

    for customer in top_items['customer_name'].unique():
        cust_items = top_items[top_items['customer_name'] == customer].sort_values('qty', ascending=False).head(5)
        for _, row in cust_items.iterrows():
            # daily predicted units (total qty / forecast_days)
            daily_qty = round(row['qty'] / forecast_days, 1)
            for date in future_dates:
                forecast_list.append({
                    'customer_name': customer,
                    'item_name': row['item_name'],
                    'predicted_date': date,
                    'predicted_units': daily_qty
                })

    return pd.DataFrame(forecast_list)

