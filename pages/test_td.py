import streamlit as st
from twelvedata import TDClient

st.title("Twelve Data — NSE Symbol Test")

apikey = st.secrets["TWELVE_DATA_KEY"]
td = TDClient(apikey=apikey)

test_symbol = st.text_input("NSE symbol to test (no suffix)", value="RELIANCE")

if st.button("Test Fetch"):
    try:
        ts = td.time_series(
            symbol=test_symbol,
            exchange="NSE",
            interval="1day",
            outputsize=5,
        ).as_pandas()
        st.success(f"✅ Success — got {len(ts)} rows for {test_symbol}")
        st.dataframe(ts, use_container_width=True)
    except Exception as e:
        st.error(f"❌ Failed: {e}")
