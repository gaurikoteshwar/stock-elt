import pandas as pd
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

# Database credentials
USERNAME = "postgres"
PASSWORD = "lily1234"
HOST = "localhost"
PORT = "5432"
DBNAME = "stock_etl"

# Create SQLAlchemy engine
engine = create_engine(f"postgresql+psycopg2://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}")

# Load stock KPIs
df = pd.read_sql("SELECT * FROM analytics.stock_kpis_daily WHERE symbol='NVDA' ORDER BY date", engine)

#print(df.head())

# Visualization-1: Line plot of close_price over time for NVDA:

# Assuming your DataFrame is called df
df_nvda = df[df['symbol'] == 'NVDA']
df_nvda['date'] = pd.to_datetime(df_nvda['date']).dt.date

plt.figure(figsize=(12, 6))
sns.lineplot(x='date', y='close_price', data=df_nvda)
plt.title('NVDA Close Price Over Time')
plt.xlabel('Date')
plt.ylabel('Close Price')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# Visualization 2: NVDA Close Price and Rolling Sharpe Ratio

plt.figure(figsize=(12, 6))

# Line plot for close price
ax1 = sns.lineplot(x='date', y='close_price', data=df_nvda, label='Close Price', color='blue')
ax1.set_xlabel('Date')
ax1.set_ylabel('Close Price', color='blue')
ax1.tick_params(axis='y', labelcolor='blue')

# Twin axis for rolling Sharpe
ax2 = ax1.twinx()
sns.lineplot(x='date', y='rolling_sharpe_20d', data=df_nvda, label='Rolling Sharpe 20d', color='orange', ax=ax2)
ax2.set_ylabel('Rolling Sharpe 20d', color='orange')
ax2.tick_params(axis='y', labelcolor='orange')

# Format x-axis to show all dates neatly
ax1.xaxis.set_major_locator(mdates.AutoDateLocator())
ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
plt.setp(ax1.get_xticklabels(), rotation=45, ha='right')

plt.title('NVDA Close Price and Rolling Sharpe Ratio')
plt.tight_layout()
plt.show()

#Visualization 3: how the stock’s risk (volatility) changes over time

plt.figure(figsize=(12, 5))
sns.lineplot(x='date', y='rolling_vol_20d', data=df_nvda, color='red')
plt.xlabel('Date')
plt.ylabel('20-Day Rolling Volatility')
plt.title('NVDA 20-Day Rolling Volatility')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

#Visualization 4: shows the cumulative buying/selling pressure—volume flow relative to price movement - obv

plt.figure(figsize=(12, 5))
sns.lineplot(x='date', y='obv', data=df_nvda, color='purple')
plt.xlabel('Date')
plt.ylabel('On-Balance Volume (OBV)')
plt.title('NVDA On-Balance Volume')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

#Visualization 5: Analyse average monthly close prices for each year to see seasonal and yearly trends
df_nvda['date'] = pd.to_datetime(df_nvda['date'])

# Extract year and month
df_nvda['year'] = df_nvda['date'].dt.year
df_nvda['month'] = df_nvda['date'].dt.month

# Aggregate: average monthly close per year
df_monthly = df_nvda.groupby(['year', 'month'])['close_price'].mean().reset_index()

plt.figure(figsize=(14,6))
for year in sorted(df_monthly['year'].unique()):
    monthly_data = df_monthly[df_monthly['year'] == year]
    plt.plot(monthly_data['month'], monthly_data['close_price'], marker='o', label=str(year))

plt.xlabel('Month')
plt.ylabel('Average Monthly Close Price')
plt.title('NVDA: Year-over-Year Monthly Close Price Comparison')
plt.xticks(range(1,13))
plt.legend(title='Year')
plt.grid(True)
plt.tight_layout()
plt.show()

#Visualization 6: Distribution of daily returns per year, highlighting volatility differences across years.
plt.figure(figsize=(12,6))
sns.boxplot(x='year', y='daily_return', data=df_nvda)
plt.xlabel('Year')
plt.ylabel('Daily Return')
plt.title('NVDA: Distribution of Daily Returns by Year')
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

#Visualization 7: interactive line chart for NVDA close price and OBV:

import plotly.graph_objects as go

# Interactive figure
fig = go.Figure()

# Close Price line
fig.add_trace(go.Scatter(
    x=df_nvda['date'],
    y=df_nvda['close_price'],
    mode='lines+markers',
    name='Close Price',
    line=dict(color='blue')
))

# OBV line on secondary y-axis
fig.add_trace(go.Scatter(
    x=df_nvda['date'],
    y=df_nvda['obv'],
    mode='lines',
    name='OBV',
    line=dict(color='orange'),
    yaxis='y2'
))

# Layout
fig.update_layout(
    title='NVDA Close Price and OBV (Interactive)',
    xaxis_title='Date',
    yaxis_title='Close Price',
    yaxis2=dict(
        title='OBV',
        overlaying='y',
        side='right'
    ),
    legend=dict(x=0.01, y=0.99),
    hovermode='x unified',
    template='plotly_white'
)

fig.show()
