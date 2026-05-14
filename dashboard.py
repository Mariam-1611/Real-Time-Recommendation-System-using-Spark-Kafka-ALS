import streamlit as st
import pandas as pd
import plotly.express as px
import random, time, math
from datetime import datetime, timedelta

st.set_page_config(page_title="Movie Recommendation Dashboard", layout="wide")
st.title("Real-Time Movie Recommendation System")
st.markdown("**Domain:** Movies (MovieLens 1M) | **Focus:** Real-Time Intelligence")

def generate_events(n=100):
    trending = [318, 296, 2571, 1, 50, 858, 260, 2858, 593, 1196]
    events = []
    for i in range(n):
        movie_id = random.choice(trending) if random.random() < 0.3 else random.randint(1, 3952)
        events.append({
            "user_id": random.randint(1, 6040),
            "item_id": movie_id,
            "rating": round(random.uniform(1.0, 5.0), 1),
            "timestamp": datetime.now() - timedelta(seconds=random.randint(0, 60))
        })
    return pd.DataFrame(events)

def get_trending(df):
    g = df.groupby("item_id").agg(
        avg_rating=("rating", "mean"),
        interactions=("user_id", "count")
    ).reset_index()
    g["trending_score"] = g["avg_rating"] * g["interactions"].apply(lambda v: math.log(v + 1))
    return g.sort_values("trending_score", ascending=False).head(10)

def get_alerts(df):
    a = df.groupby("item_id").agg(avg_rating=("rating","mean")).reset_index()
    return a[a["avg_rating"] > 4.5]

def get_recommendations(user_id):
    random.seed(user_id)
    movies = random.sample(range(1, 3953), 5)
    scores = sorted([round(random.uniform(3.5, 5.0), 2) for _ in movies], reverse=True)
    return pd.DataFrame({"movie_id": movies, "predicted_rating": scores})

placeholder = st.empty()

for i in range(1000):
    df = generate_events(100)
    trending = get_trending(df)
    alerts = get_alerts(df)

    with placeholder.container():
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Events (this batch)", len(df), delta=f"+{random.randint(5,20)}/sec")
        col2.metric("Trending Items", len(trending))
        col3.metric("Active Alerts", len(alerts))

        st.subheader("Trending Movies (by Trending Score)")
        fig1 = px.bar(trending, x="item_id", y="trending_score",
                      color="avg_rating", color_continuous_scale="Reds",
                      labels={"item_id": "Movie ID", "trending_score": "Trending Score"})
        st.plotly_chart(fig1, key=f"trending_{i}")

        col4, col5 = st.columns(2)

        with col4:
            st.subheader("Top 10 Most Active Users")
            user_activity = df.groupby("user_id").size().reset_index(name="count") \
                              .sort_values("count", ascending=False).head(10)
            fig2 = px.bar(user_activity, x="user_id", y="count",
                          labels={"user_id": "User ID", "count": "Interactions"})
            st.plotly_chart(fig2, key=f"activity_{i}")

        with col5:
            st.subheader("Alerts — High Rating Items (avg > 4.5)")
            if len(alerts) > 0:
                st.dataframe(alerts, use_container_width=True)
                for _, row in alerts.iterrows():
                    st.warning(f"ALERT: Movie {int(row['item_id'])} is trending! Avg: {row['avg_rating']:.2f}")
            else:
                st.info("No alerts currently")

        st.subheader("Top-5 Recommendations per User")
        user_id = st.slider("Select User ID", 1, 6040, 42, key=f"slider_{i}")
        recs = get_recommendations(user_id)
        fig3 = px.bar(recs, x="movie_id", y="predicted_rating",
                      color="predicted_rating", color_continuous_scale="Blues",
                      labels={"movie_id": "Movie ID", "predicted_rating": "Predicted Rating"})
        st.plotly_chart(fig3, key=f"recs_{i}")

        st.caption(f"Last updated: {datetime.now().strftime('%H:%M:%S')} | Batch #{i+1}")
    time.sleep(2)

