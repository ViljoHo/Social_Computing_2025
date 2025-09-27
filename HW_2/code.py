import sqlite3
import pandas as pd
import matplotlib.pyplot as plt

DB_FILE = "database.sqlite"
try:
    con = sqlite3.connect(DB_FILE)
    print("Connection successful")

    users = pd.read_sql_query("SELECT * FROM users", con)
    posts = pd.read_sql_query("SELECT * FROM posts", con)
    comments = pd.read_sql_query("SELECT * FROM comments", con)
    reactions = pd.read_sql_query("SELECT * FROM reactions", con)
except Exception as e:
    print(f"{e}")
if con:
        con.close()
        print("SQLite Database connection closed.")

# -- Task 2.1 pandas --

users['created_at'] = pd.to_datetime(users['created_at'])

# Extract year-month as period
users['month'] = users['created_at'].dt.to_period('M')

# Count how many users in each month
users_counts_per_month = users.groupby('month').size()

# Build full monthly range from min to max
all_months = pd.period_range(users['month'].min(), users['month'].max(), freq='M')

# Reindex to include missing months, fill with 0
users_counts_per_month = users_counts_per_month.reindex(all_months, fill_value=0)

# Convert to DataFrame
monthly_users_counts = users_counts_per_month.reset_index()
monthly_users_counts.columns = ['month', 'monthly_users']

# Add cumulative sum
monthly_users_counts['cumulative_users'] = monthly_users_counts['monthly_users'].cumsum()



print("Monthly and Cumulative users")
print(monthly_users_counts)

# Plot
plt.plot(monthly_users_counts['month'].astype(str), monthly_users_counts['cumulative_users'])
plt.title("Cumulative Users On The DataBase Over Time")
plt.xlabel("Month")
plt.ylabel("Total Cumulative Users")
plt.xticks(rotation=45)
plt.grid(True)
plt.show()

# Naive users growth estimate
current_users = monthly_users_counts['cumulative_users'].iloc[-1]
n_months = len(monthly_users_counts)
avg_growth = current_users / n_months
predicted_total_3_years = current_users + avg_growth * 36
usage_per_server = current_users/16
prediction_plus_20_percent = (predicted_total_3_years / usage_per_server) * 1.2

print(
    f"\nPrediction: There is {avg_growth:.2f} users/month historical growth, "
    f"\nso based on that there will be approximately {predicted_total_3_years:.0f} users after 3 years"
    f"\nand currently there is 16 servers handling {current_users} users, {usage_per_server:.2f} users/server"
    f"\nso there should be {prediction_plus_20_percent:.0f} servers to handle that {predicted_total_3_years:.0f} predicted numbers of users"
    f"\n(including 20% capacity for redundancy)"
)






con.close()

