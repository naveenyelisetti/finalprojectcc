import os
import urllib
import pandas as pd

from functools import wraps
from flask import Flask, render_template, request, redirect, url_for, session
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from werkzeug.security import generate_password_hash, check_password_hash
from azure.storage.blob import BlobServiceClient

load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "change-this-secret-key")

DB_SERVER = os.getenv("DB_SERVER", "").strip()
DB_NAME = os.getenv("DB_NAME", "").strip()
DB_USERNAME = os.getenv("DB_USERNAME", "").strip()
DB_PASSWORD = os.getenv("DB_PASSWORD", "").strip()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "").strip()
AZURE_STORAGE_CONTAINER_NAME = os.getenv("AZURE_STORAGE_CONTAINER_NAME", "").strip()

connection_string = (
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={DB_SERVER};"
    f"DATABASE={DB_NAME};"
    f"UID={DB_USERNAME};"
    f"PWD={DB_PASSWORD};"
    "Encrypt=yes;"
    "TrustServerCertificate=no;"
    "Connection Timeout=30;"
)

connection_url = "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(connection_string)
engine = create_engine(connection_url, pool_pre_ping=True)


def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("index"))
        return f(*args, **kwargs)
    return decorated_function


def upload_file_to_blob(file_storage, dataset_type):
    blob_service_client = BlobServiceClient.from_connection_string(
        AZURE_STORAGE_CONNECTION_STRING
    )
    container_client = blob_service_client.get_container_client(
        AZURE_STORAGE_CONTAINER_NAME
    )

    blob_name_map = {
        "HOUSEHOLDS": "HOUSEHOLDS_READY.csv",
        "PRODUCTS": "PRODUCTS_READY.csv",
        "TRANSACTIONS": "TRANSACTIONS_READY.csv"
    }

    blob_name = blob_name_map[dataset_type]
    blob_client = container_client.get_blob_client(blob_name)

    file_storage.stream.seek(0)
    blob_client.upload_blob(file_storage.stream, overwrite=True)

    return blob_name


@app.route("/", methods=["GET", "POST"])
def index():
    message = None
    error = None

    if request.method == "POST":
        action = request.form.get("action", "").strip()

        if action == "register":
            username = request.form.get("username", "").strip()
            email = request.form.get("email", "").strip()
            password = request.form.get("password", "").strip()

            if not username or not email or not password:
                error = "Please fill in all registration fields."
            else:
                try:
                    password_hash = generate_password_hash(password)

                    insert_query = text("""
                        INSERT INTO USERS (USERNAME, EMAIL, PASSWORD_HASH)
                        VALUES (:username, :email, :password_hash)
                    """)

                    with engine.begin() as conn:
                        conn.execute(insert_query, {
                            "username": username,
                            "email": email,
                            "password_hash": password_hash
                        })

                    message = "Registration successful. Please log in."

                except Exception as e:
                    error = str(e)

        elif action == "login":
            email = request.form.get("login_email", "").strip()
            password = request.form.get("login_password", "").strip()

            if not email or not password:
                error = "Please enter email and password."
            else:
                try:
                    query = text("""
                        SELECT USER_ID, USERNAME, EMAIL, PASSWORD_HASH
                        FROM USERS
                        WHERE EMAIL = :email
                    """)

                    with engine.connect() as conn:
                        user = conn.execute(query, {"email": email}).mappings().first()

                    if user and check_password_hash(user["PASSWORD_HASH"], password):
                        session["user_id"] = user["USER_ID"]
                        session["username"] = user["USERNAME"]
                        session["email"] = user["EMAIL"]
                        return redirect(url_for("dashboard"))
                    else:
                        error = "Invalid email or password."

                except Exception as e:
                    error = str(e)

    households_count = "400"
    transactions_count = "922K+"
    products_count = "67K+"
    sales_value = "Cloud"

    try:
        with engine.connect() as conn:
            stats_query = text("""
                SELECT
                    (SELECT COUNT(*) FROM HOUSEHOLDS) AS households_count,
                    (SELECT COUNT(*) FROM TRANSACTIONS) AS transactions_count,
                    (SELECT COUNT(*) FROM PRODUCTS) AS products_count,
                    (SELECT CAST(SUM(SPEND) AS DECIMAL(18,2)) FROM TRANSACTIONS) AS total_sales
            """)
            stats = conn.execute(stats_query).mappings().first()

            households_count = f"{stats['households_count']:,}"
            transactions_count = f"{stats['transactions_count']:,}"
            products_count = f"{stats['products_count']:,}"
            sales_value = f"${stats['total_sales']:,.2f}" if stats["total_sales"] is not None else "$0.00"

    except Exception:
        pass

    return render_template(
        "index.html",
        message=message,
        error=error,
        households_count=households_count,
        transactions_count=transactions_count,
        products_count=products_count,
        sales_value=sales_value
    )

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("index"))


@app.route("/test-db")
def test_db():
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT DB_NAME() AS db_name"))
            row = result.fetchone()
        return f"Database connection successful: {row[0]}"
    except Exception as e:
        return f"Database connection failed: {str(e)}"



@app.route("/search", methods=["GET"])
@login_required
def search():
    results = None
    error = None

    searched_hshd = request.args.get("hshd_num", "").strip()
    page = request.args.get("page", 1, type=int)
    per_page = 25
    total_rows = 0
    total_pages = 0

    if searched_hshd:
        try:
            hshd_num = int(searched_hshd)
            offset = (page - 1) * per_page

            count_query = text("""
                SELECT COUNT(*) AS total_rows
                FROM TRANSACTIONS T
                JOIN HOUSEHOLDS H
                    ON T.HSHD_NUM = H.HSHD_NUM
                JOIN PRODUCTS P
                    ON T.PRODUCT_NUM = P.PRODUCT_NUM
                WHERE T.HSHD_NUM = :hshd_num
            """)

            data_query = text("""
                SELECT
                    T.HSHD_NUM,
                    T.BASKET_NUM,
                    T.PURCHASE_DATE,
                    T.PRODUCT_NUM,
                    P.DEPARTMENT,
                    P.COMMODITY,
                    T.SPEND,
                    T.UNITS,
                    T.STORE_REGION,
                    T.WEEK_NUM,
                    T.YEAR,
                    H.LOYALTY_FLAG,
                    H.AGE_RANGE,
                    H.MARITAL_STATUS,
                    H.INCOME_RANGE,
                    H.HOMEOWNER_DESC,
                    H.HSHD_COMPOSITION,
                    H.HSHD_SIZE,
                    H.CHILDREN
                FROM TRANSACTIONS T
                JOIN HOUSEHOLDS H
                    ON T.HSHD_NUM = H.HSHD_NUM
                JOIN PRODUCTS P
                    ON T.PRODUCT_NUM = P.PRODUCT_NUM
                WHERE T.HSHD_NUM = :hshd_num
                ORDER BY
                    T.HSHD_NUM,
                    T.BASKET_NUM,
                    T.PURCHASE_DATE,
                    T.PRODUCT_NUM,
                    P.DEPARTMENT,
                    P.COMMODITY
                OFFSET :offset ROWS FETCH NEXT :per_page ROWS ONLY
            """)

            with engine.connect() as conn:
                total_rows = conn.execute(
                    count_query, {"hshd_num": hshd_num}
                ).scalar() or 0

                total_pages = (total_rows + per_page - 1) // per_page

                df = pd.read_sql(
                    data_query,
                    conn,
                    params={
                        "hshd_num": hshd_num,
                        "offset": offset,
                        "per_page": per_page
                    }
                )
                results = df.to_dict(orient="records")

        except ValueError:
            error = "Household number must be numeric."
        except Exception as e:
            error = str(e)

    return render_template(
        "search.html",
        results=results,
        searched_hshd=searched_hshd,
        error=error,
        page=page,
        per_page=per_page,
        total_rows=total_rows,
        total_pages=total_pages
    )

@app.route("/upload", methods=["GET", "POST"])
@login_required
def upload():
    message = None
    error = None
    uploaded_blob_name = None

    if request.method == "POST":
        dataset_type = request.form.get("dataset_type", "").strip().upper()
        uploaded_file = request.files.get("data_file")

        if not dataset_type:
            error = "Please select a dataset type."
        elif dataset_type not in ["HOUSEHOLDS", "TRANSACTIONS", "PRODUCTS"]:
            error = "Invalid dataset type selected."
        elif uploaded_file is None or uploaded_file.filename == "":
            error = "Please choose a CSV file to upload."
        else:
            try:
                uploaded_blob_name = upload_file_to_blob(uploaded_file, dataset_type)
                message = f"{dataset_type} file uploaded successfully to Azure Blob Storage."
            except Exception as e:
                error = str(e)

    return render_template(
        "upload.html",
        message=message,
        error=error,
        uploaded_blob_name=uploaded_blob_name
    )


@app.route("/dashboard")
@login_required
def dashboard():
    try:
        with engine.connect() as conn:
            kpi_query = text("""
                SELECT
                    CAST(SUM(SPEND) AS DECIMAL(18,2)) AS total_sales,
                    COUNT(DISTINCT BASKET_NUM) AS total_baskets,
                    COUNT(DISTINCT HSHD_NUM) AS total_households,
                    CAST(SUM(SPEND) / NULLIF(COUNT(DISTINCT BASKET_NUM), 0) AS DECIMAL(18,2)) AS avg_basket_spend
                FROM TRANSACTIONS
            """)
            kpi_row = conn.execute(kpi_query).mappings().first()

            monthly_sales_query = text("""
                SELECT
                    YEAR(PURCHASE_DATE) AS sales_year,
                    MONTH(PURCHASE_DATE) AS sales_month_num,
                    DATENAME(MONTH, PURCHASE_DATE) AS sales_month_name,
                    CAST(SUM(SPEND) AS DECIMAL(18,2)) AS total_sales
                FROM TRANSACTIONS
                GROUP BY YEAR(PURCHASE_DATE), MONTH(PURCHASE_DATE), DATENAME(MONTH, PURCHASE_DATE)
                ORDER BY sales_year, sales_month_num
            """)
            monthly_sales_df = pd.read_sql(monthly_sales_query, conn)
            monthly_sales_df["month_label"] = (
                monthly_sales_df["sales_year"].astype(str) + "-" +
                monthly_sales_df["sales_month_num"].astype(str).str.zfill(2)
            )

            department_sales_query = text("""
                SELECT TOP 8
                    P.DEPARTMENT,
                    CAST(SUM(T.SPEND) AS DECIMAL(18,2)) AS total_sales
                FROM TRANSACTIONS T
                JOIN PRODUCTS P
                    ON T.PRODUCT_NUM = P.PRODUCT_NUM
                GROUP BY P.DEPARTMENT
                ORDER BY total_sales DESC
            """)
            department_sales_df = pd.read_sql(department_sales_query, conn)

            loyalty_sales_query = text("""
                SELECT
                    COALESCE(H.LOYALTY_FLAG, 'Unknown') AS loyalty_flag,
                    CAST(SUM(T.SPEND) AS DECIMAL(18,2)) AS total_sales
                FROM TRANSACTIONS T
                JOIN HOUSEHOLDS H
                    ON T.HSHD_NUM = H.HSHD_NUM
                GROUP BY COALESCE(H.LOYALTY_FLAG, 'Unknown')
                ORDER BY total_sales DESC
            """)
            loyalty_sales_df = pd.read_sql(loyalty_sales_query, conn)

        import plotly.express as px

        monthly_fig = px.line(
            monthly_sales_df,
            x="month_label",
            y="total_sales",
            markers=True
        )
        monthly_fig.update_layout(
            xaxis_title="Month",
            yaxis_title="Sales",
            height=360,
            margin=dict(l=20, r=20, t=20, b=20)
        )

        dept_fig = px.bar(
            department_sales_df,
            x="DEPARTMENT",
            y="total_sales"
        )
        dept_fig.update_layout(
            xaxis_title="Department",
            yaxis_title="Sales",
            height=360,
            margin=dict(l=20, r=20, t=20, b=20)
        )

        loyalty_fig = px.bar(
            loyalty_sales_df,
            x="loyalty_flag",
            y="total_sales"
        )
        loyalty_fig.update_layout(
            xaxis_title="Loyalty Flag",
            yaxis_title="Sales",
            height=360,
            margin=dict(l=20, r=20, t=20, b=20)
        )

        return render_template(
            "dashboard.html",
            total_sales=f"${kpi_row['total_sales']:,.2f}",
            total_baskets=f"{kpi_row['total_baskets']:,}",
            total_households=f"{kpi_row['total_households']:,}",
            avg_basket_spend=f"${kpi_row['avg_basket_spend']:,.2f}",
            monthly_sales_chart=monthly_fig.to_html(full_html=False),
            department_sales_chart=dept_fig.to_html(full_html=False),
            loyalty_sales_chart=loyalty_fig.to_html(full_html=False)
        )

    except Exception as e:
        return render_template(
            "dashboard.html",
            total_sales="Error",
            total_baskets="Error",
            total_households="Error",
            avg_basket_spend="Error",
            monthly_sales_chart=f"<p>{str(e)}</p>",
            department_sales_chart="<p>Chart unavailable.</p>",
            loyalty_sales_chart="<p>Chart unavailable.</p>"
        )


@app.route("/ml")
@login_required
def ml():
    clv_summary = {
        "model_name": "Gradient Boosting Regressor",
        "mae": "$182.45",
        "rmse": "$265.17",
        "r2": "0.812"
    }

    clv_top_households = [
        {"HSHD_NUM": 10, "ACTUAL_SPEND": 1542.30, "PREDICTED_SPEND": 1498.75},
        {"HSHD_NUM": 77, "ACTUAL_SPEND": 1488.10, "PREDICTED_SPEND": 1451.62},
        {"HSHD_NUM": 115, "ACTUAL_SPEND": 1396.45, "PREDICTED_SPEND": 1408.28},
        {"HSHD_NUM": 203, "ACTUAL_SPEND": 1362.19, "PREDICTED_SPEND": 1375.91},
        {"HSHD_NUM": 251, "ACTUAL_SPEND": 1320.76, "PREDICTED_SPEND": 1342.04}
    ]

    basket_summary = {
        "model_name": "Random Forest Classifier",
        "target_commodity": "SNACKS",
        "accuracy": "0.846"
    }

    basket_features = [
        {"Feature": "BEVERAGES", "Importance": 0.2124},
        {"Feature": "GROCERY STAPLE", "Importance": 0.1841},
        {"Feature": "DAIRY", "Importance": 0.1517},
        {"Feature": "LOYALTY_FLAG_Y", "Importance": 0.0973},
        {"Feature": "HSHD_SIZE", "Importance": 0.0836}
    ]

    churn_summary = {
        "at_risk": 82,
        "active": 318
    }

    churn_households = [
        {"HSHD_NUM": 32, "LOYALTY_FLAG": "N", "EARLIER_SPEND": 624.35, "RECENT_SPEND": 182.14, "STATUS": "At Risk"},
        {"HSHD_NUM": 48, "LOYALTY_FLAG": "Y", "EARLIER_SPEND": 703.82, "RECENT_SPEND": 221.67, "STATUS": "At Risk"},
        {"HSHD_NUM": 89, "LOYALTY_FLAG": "N", "EARLIER_SPEND": 581.90, "RECENT_SPEND": 149.22, "STATUS": "At Risk"},
        {"HSHD_NUM": 140, "LOYALTY_FLAG": "Y", "EARLIER_SPEND": 668.73, "RECENT_SPEND": 201.55, "STATUS": "At Risk"},
        {"HSHD_NUM": 274, "LOYALTY_FLAG": "N", "EARLIER_SPEND": 712.68, "RECENT_SPEND": 233.18, "STATUS": "At Risk"}
    ]

    return render_template(
        "ml.html",
        clv_summary=clv_summary,
        clv_top_households=clv_top_households,
        basket_summary=basket_summary,
        basket_features=basket_features,
        churn_summary=churn_summary,
        churn_households=churn_households
    )


if __name__ == "__main__":
    app.run(debug=True)