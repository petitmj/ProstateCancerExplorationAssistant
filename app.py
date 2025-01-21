import streamlit as st
from snowflake.snowpark.context import get_active_session
import pandas as pd

# Streamlit page configuration
st.set_page_config(page_title="Prostate Cancer Exploration Assistant")
st.title("Prostate Cancer Exploration Assistant")
st.markdown("""
This app retrieves documents using Cortex AI for retrieval. It generates insights 
using the Mistral LLM for exploring resistance mechanisms in prostate cancer, particularly AR-V7.
""")

# Initialize Snowflake session
session = get_active_session()

# Database and table configurations
DATABASE = "RESISTANCE_MECHANISM_ANALYSIS_DB"
SCHEMA = "PUBLIC"
DOCUMENTS_TABLE = "DOCUMENTS"
INSIGHTS_TABLE = "INSIGHTS"
FEEDBACK_TABLE = "FEEDBACKLOGS"
QUERIES_TABLE = "QUERIES"
NUM_CHUNKS = 3  # Number of chunks retrieved for context

# Function to fetch documents via Cortex AI search
def fetch_documents_cortex(query_text):
    """
    Fetch documents using Cortex AI search.
    """
    try:
        sql = f"""
            SELECT snowflake.cortex.search(
                model_name => 'mistral-search',
                query => ?,
                data_table => '{DATABASE}.{SCHEMA}.{DOCUMENTS_TABLE}',
                fields => ARRAY['CONTENT', 'TITLE'],
                top_k => {NUM_CHUNKS}
            ) AS DOCUMENTS;
        """
        data = session.sql(sql, params=[query_text]).collect()
        return [row["DOCUMENTS"] for row in data] if data else []
    except Exception:
        return []  

# Function to fetch documents directly via SQL query
def fetch_documents_sql(query_text):
    """
    Fetch documents using full-text search with CONTAINS.
    """
    try:
        sql = f"""
            SELECT TITLE, CONTENT, QUERYID
            FROM {DATABASE}.{SCHEMA}.{DOCUMENTS_TABLE}
            WHERE CONTAINS(CONTENT, ?)
            LIMIT {NUM_CHUNKS};
        """
        data = session.sql(sql, params=[query_text]).to_pandas()
        return data.to_dict("records") if not data.empty else []
    except Exception as e:
        st.error(f"Error fetching documents using SQL search: {e}")
        return []

# Function to generate insights using Mistral LLM
def generate_insights(context):
    """
    Generate insights using the Snowflake Cortex Mistral LLM `COMPLETE` function.
    """
    if not context.strip():
        return "No valid context provided for generating insights."
    prompt = f"""
    You are an expert assistant providing insights based on the following context:
    Context: {context}
    Question: What are the key resistance mechanisms in prostate cancer related to AR-V7 and other therapies?
    Answer:
    """
    try:
        query = """
            SELECT snowflake.cortex.complete(?, ?) AS response
        """
        result = session.sql(query, params=["mistral-large2", prompt]).collect()
        return result[0]["RESPONSE"] if result else "No response generated."
    except Exception as e:
        return f"Error generating insights: {e}"

# Function to log the query and retrieve the QUERYID
def log_query_and_get_query_id(query_text, user_id=1):
    """
    Log the query into the QUERIES table and return the generated QUERYID.
    """
    try:
        insert_query = f"""
            INSERT INTO {DATABASE}.{SCHEMA}.{QUERIES_TABLE} (QUERYTEXT, CREATEDAT, USERID)
            VALUES (?, CURRENT_TIMESTAMP(), ?);
        """
        session.sql(insert_query, params=[query_text, user_id]).collect()
        
        select_query = f"""
            SELECT MAX(QUERYID) AS QUERYID
            FROM {DATABASE}.{SCHEMA}.{QUERIES_TABLE}
            WHERE QUERYTEXT = ? AND USERID = ?;
        """
        result = session.sql(select_query, params=[query_text, user_id]).collect()
        return result[0]["QUERYID"] if result else None
    except Exception as e:
        st.error(f"Error logging query: {e}")
        return None

# Function to log insights into the INSIGHTS table
def log_insights(insight_text, query_id):
    """
    Log the generated insights into the database INSIGHTS table.
    """
    try:
        insert_query = f"""
            INSERT INTO {DATABASE}.{SCHEMA}.{INSIGHTS_TABLE} 
            (INSIGHTTEXT, QUERYID, GENERATEDAT) 
            VALUES (?, ?, CURRENT_TIMESTAMP());
        """
        session.sql(insert_query, params=[insight_text, query_id]).collect()
        st.success("Insights logged successfully!")
    except Exception as e:
        st.error(f"Error logging insights: {e}")

# Query input section
query = st.text_input("Enter a query (e.g., 'AR-V7 resistance in prostate cancer'):",
                      placeholder="Type your query here...")
custom_context = st.text_area(
    "Or input your own context for insight generation:",
    placeholder="Paste or type the context here..."
)

# Query logging and processing section
if st.button("Fetch and Analyze"):
    if query or custom_context:
        if not custom_context:
            query_id = log_query_and_get_query_id(query)
            if query_id:
                with st.spinner("Retrieving documents..."):
                    # Try Cortex AI search first
                    documents = fetch_documents_cortex(query)
                    if not documents:
                        # Silently fallback to SQL search
                        documents = fetch_documents_sql(query)
                    
                    if documents:
                        st.markdown("### Retrieved Documents")
                        st.json(documents)
                        context = "\n".join(doc.get("CONTENT", "") for doc in documents)
                    else:
                        context = ""
        else:
            # Use the user-provided context
            query_id = None
            context = custom_context
        
        if context.strip():
            with st.spinner("Generating insights using Mistral LLM..."):
                insights = generate_insights(context)
                st.markdown("### Generated Insights")
                st.write(insights)
                if query_id:
                    log_insights(insights, query_id)
        else:
            st.warning("No valid context found or provided.")
    else:
        st.warning("Please enter a query or input your own context.")

# Feedback submission section
feedback_text = st.text_area("Submit your feedback on the insights generated:")
feedback_type = st.selectbox("Select feedback type:", ["Positive", "Negative", "Neutral"])
if st.button("Submit Feedback"):
    if feedback_text:
        try:
            insight_id = session.sql(f"""
                SELECT INSIGHTID FROM {DATABASE}.{SCHEMA}.{INSIGHTS_TABLE} 
                ORDER BY GENERATEDAT DESC LIMIT 1;
            """).collect()[0]["INSIGHTID"]
            insert_query = f"""
                INSERT INTO {DATABASE}.{SCHEMA}.{FEEDBACK_TABLE} 
                (FEEDBACKDETAILS, FEEDBACKTYPE, INSIGHTID, LOGGEDAT) 
                VALUES (?, ?, ?, CURRENT_TIMESTAMP());
            """
            session.sql(insert_query, params=[feedback_text, feedback_type, insight_id]).collect()
            st.success("Feedback submitted successfully!")
        except Exception as e:
            st.error(f"Error submitting feedback: {e}")
    else:
        st.warning("Please provide feedback before submitting.")

# View feedback logs
if st.button("View Feedback Logs"):
    with st.spinner("Loading feedback logs..."):
        try:
            sql = f"""
                SELECT FL.FEEDBACKDETAILS, FL.FEEDBACKTYPE, I.INSIGHTTEXT, FL.LOGGEDAT
                FROM {DATABASE}.{SCHEMA}.{FEEDBACK_TABLE} FL
                JOIN {DATABASE}.{SCHEMA}.{INSIGHTS_TABLE} I 
                ON FL.INSIGHTID = I.INSIGHTID
                ORDER BY FL.LOGGEDAT DESC;
            """
            logs = session.sql(sql).to_pandas()
            if not logs.empty:
                st.markdown("### Feedback Logs")
                st.dataframe(logs)
            else:
                st.write("No feedback records found.")
        except Exception as e:
            st.error(f"Error fetching feedback logs: {e}")
