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

# # -- Task 2.1 --

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

# -- Task 2.2 --

# Convert timestamps
posts["created_at"] = pd.to_datetime(posts["created_at"])
comments["created_at"] = pd.to_datetime(comments["created_at"])

# Filter comments within 2 days of post creation
merged = comments.merge(posts, left_on="post_id", right_on="id", suffixes=("_comment", "_post"))

mask = (merged["created_at_comment"] <= merged["created_at_post"] + pd.Timedelta(days=3))
filtered_comments = merged[mask]

# Count comments per post
comment_counts = filtered_comments.groupby("post_id").size().reset_index(name="comment_count")

# Get top 20 posts by comments
top_posts = comment_counts.sort_values("comment_count", ascending=False).head(20)

# Count reactions for those posts
reaction_counts = reactions.groupby("post_id").size().reset_index(name="reaction_count")

# Merge reaction counts into top_posts
top_posts = top_posts.merge(reaction_counts, on="post_id", how="left").fillna(0)

# Merge with post content for readability
top_posts = top_posts.merge(posts[["id", "content"]], left_on="post_id", right_on="id", how="left")
top_posts = top_posts.drop(columns="id")

print("Top 20 posts with most comments in first 3 days and their reaction counts:")
print(top_posts)

# Add weighted virality score
top_posts["virality_score"] = top_posts["comment_count"] * 0.7 + top_posts["reaction_count"] * 0.3

# Sort by virality score
ranked_posts = top_posts.sort_values("virality_score", ascending=False).reset_index(drop=True).head(3)

print("Posts ranked by weighted score (70% comments, 30% reactions):")
print(ranked_posts[["post_id", "comment_count", "reaction_count", "virality_score", "content"]])


# -- Task 2.3 --
# Convert timestamps
posts["created_at"] = pd.to_datetime(posts["created_at"])
comments["created_at"] = pd.to_datetime(comments["created_at"])

# Filter comments that are created before post, How could this even be possible????
merged = comments.merge(posts, left_on="post_id", right_on="id", suffixes=("_comment", "_post"))

mask = (merged["created_at_comment"] >= merged["created_at_post"])
filtered_comments = merged[mask]

# Calculate time between post and comment
filtered_comments["time_between"] = filtered_comments["created_at_comment"] - filtered_comments["created_at_post"]

times = filtered_comments.groupby("post_id")["time_between"].max().reset_index(name="min/max_time")

# Convert times to seconds for easier to read
times["min/max_time"] = times["min/max_time"].dt.total_seconds()

# Add H/M/S format
times["formatted"] = times["min/max_time"].apply(
    lambda x: f"{int(x//3600)}h {int((x%3600)//60)}m {int(x%60)}s"
)

print(merged)

print(times)

# Calculate average
avg_seconds = times["min/max_time"].mean()

days = int(avg_seconds // 86400)
hours = int((avg_seconds % 86400) // 3600)
minutes = int((avg_seconds % 3600) // 60)
seconds = int(avg_seconds % 60)

formatted_avg = f"{days}d {hours}h {minutes}m {seconds}s"

print(f'Average time: {formatted_avg}')


# -- Task 2.4 --






con.close()

