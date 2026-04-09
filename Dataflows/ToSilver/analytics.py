from sklearn.cluster import KMeans

# ==============================================================================
# ANALYTICS
# ==============================================================================

def analytics(df):
    """
    Simple analysis examples:
    1. Descriptive statistics: Closing price distribution for each stock
    2. Volatility: Standard deviation of returns (measures risk)
    3. K-Means clustering: Automatically divides stocks into 3 categories
    """
    print("\n📊 Descriptive Stats")
    print(df.groupby("ticker")["close"].describe())

    print("\n📉 Volatility")
    df["returns"] = df.groupby("ticker")["close"].pct_change()
    print(df.groupby("ticker")["returns"].std())

    print("\n📊 Clustering")
    features = df.groupby("ticker")[["close","volume"]].mean()

    kmeans = KMeans(n_clusters=3, random_state=42).fit(features)
    features["cluster"] = kmeans.labels_

    print(features)
