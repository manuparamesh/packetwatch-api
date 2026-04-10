"""
PacketWatch FastAPI Backend
---------------------------
Fully server-side pipeline: Gmail OAuth → parse → estimate → return stats
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import uuid, os, json, base64, email, asyncio
from email import policy
from datetime import datetime

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
from openai import AzureOpenAI
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

app = FastAPI(
    title="PacketWatch API",
    description="Estimate plastic waste from your food delivery order history.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory job store (replace with Redis for production scale)
jobs: dict = {}

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

GOOGLE_CLIENT_CONFIG = {
    "web": {
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "redirect_uris": [os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")],
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
}

azure_client = AzureOpenAI(
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_version=os.getenv("AZURE_OPENAI_API_VERSION"),
)
DEPLOYMENT = os.getenv("AZURE_OPENAI_DEPLOYMENT")

# ── SYSTEM PROMPT ───────────────────────────────────────────────────────────
SYSTEM_PROMPT = """
You are a packaging waste expert for Indian food delivery (Zomato/Swiggy in India).
Your job is to estimate PLASTIC waste only — not paper, cardboard, or foil.

Given a restaurant name and ordered items, return a JSON object:
{
  "restaurant_type": <string>,
  "containers": <int>,
  "lids": <int>,
  "cutlery_pieces": <int>,
  "outer_bags": <int>,
  "plastic_covers": <int>,
  "sauce_sachets": <int>,
  "total_grams": <float>,
  "reasoning": <string>
}

restaurant_type must be ONE of: "biryani", "burger_fast_food", "bakery_dessert",
"south_indian", "cafe_coffee", "roll_wrap_shawarma", "dhaba_northindian",
"healthy_bowl", "sandwich_sub", "pizza", "chinese_asian", "seafood",
"multicuisine", "juice_beverage", "ice_cream", "breakfast"

WEIGHTS: container=15g, lid=5g, cutlery=3g, bag=8g, cover=4g, sachet=2g

RULES:
1. Flat breads (Roti/Dosa/Idiyappam/Puttu) packed together → plastic_covers=1, containers=0
2. Burger/fast food chains → paper packaging, containers=0, outer_bags=0
3. Rolls/Wraps/Shawarma → foil, containers=0
4. Biryani → add plastic_covers=2 for raita+pickle per portion
5. Cakes/Pastries/Brownies → cardboard, containers=0
6. Ice cream scoops <200ml → containers=1+lid per item; bars → plastic_covers=1
7. Cold drinks in plastic cups → containers=1+lid; hot drinks → paper, containers=0
8. outer_bags=1 for most; 0 for burger/sandwich chains
9. Cutlery: rice/curry=1 spoon per portion; finger food/wraps/sandwiches=0

Return ONLY valid JSON. No markdown.
"""

# ── ROUTES ──────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "PacketWatch API running", "docs": "/docs"}


@app.get("/auth/url")
def get_auth_url(redirect_uri: Optional[str] = None):
    """Step 1: Get Google OAuth URL to redirect user to."""
    uri = redirect_uri or os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")
    config = json.loads(json.dumps(GOOGLE_CLIENT_CONFIG))
    config["web"]["redirect_uris"] = [uri]

    flow = Flow.from_client_config(config, scopes=SCOPES)
    flow.redirect_uri = uri
    auth_url, state = flow.authorization_url(
        access_type="offline", include_granted_scopes="true"
    )
    return {"auth_url": auth_url, "state": state}


class CallbackRequest(BaseModel):
    code: str
    state: str
    apps: list[str] = ["zomato"]   # ["zomato", "swiggy", "uber_eats"]
    redirect_uri: Optional[str] = None


@app.post("/auth/callback")
def auth_callback(req: CallbackRequest, background_tasks: BackgroundTasks):
    """Step 2: Exchange code for token, start pipeline in background."""
    uri = req.redirect_uri or os.getenv("REDIRECT_URI", "http://localhost:8000/auth/callback")
    config = json.loads(json.dumps(GOOGLE_CLIENT_CONFIG))
    config["web"]["redirect_uris"] = [uri]

    flow = Flow.from_client_config(config, scopes=SCOPES, state=req.state)
    flow.redirect_uri = uri

    try:
        flow.fetch_token(code=req.code)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"OAuth error: {e}")

    creds = flow.credentials
    job_id = str(uuid.uuid4())
    jobs[job_id] = {"status": "processing", "progress": 0, "result": None}

    background_tasks.add_task(run_pipeline, job_id, creds, req.apps)
    return {"job_id": job_id}


@app.get("/job/{job_id}")
def get_job_status(job_id: str):
    """Step 3: Poll this to check progress."""
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = jobs[job_id]
    return {
        "status": job["status"],
        "progress": job.get("progress", 0),
        "message": job.get("message", ""),
        "result": job["result"] if job["status"] == "done" else None
    }


# ── PIPELINE ────────────────────────────────────────────────────────────────

def run_pipeline(job_id: str, creds: Credentials, apps: list[str]):
    try:
        service = build("gmail", "v1", credentials=creds)
        all_orders = []

        if "zomato" in apps:
            jobs[job_id]["message"] = "Fetching Zomato orders..."
            zomato = fetch_zomato_orders(service)
            all_orders.extend(zomato)

        if "swiggy" in apps:
            jobs[job_id]["message"] = "Fetching Swiggy orders..."
            swiggy = fetch_swiggy_orders(service)
            all_orders.extend(swiggy)

        if not all_orders:
            jobs[job_id] = {"status": "done", "result": {"orders": 0, "stats": None}}
            return

        jobs[job_id]["message"] = f"Estimating plastic for {len(all_orders)} orders..."
        results = []
        for i, order in enumerate(all_orders):
            estimate = estimate_plastic(order["restaurant"], order["items_str"])
            order.update(estimate)
            results.append(order)
            jobs[job_id]["progress"] = int((i + 1) / len(all_orders) * 100)

        jobs[job_id]["message"] = "Building your report..."
        stats = compute_stats(results)
        jobs[job_id] = {"status": "done", "progress": 100, "result": stats}

    except Exception as e:
        jobs[job_id] = {"status": "error", "message": str(e), "result": None}


def fetch_zomato_orders(service) -> list[dict]:
    query = 'from:noreply@zomato.com subject:"Your Zomato order from"'
    messages = []
    result = service.users().messages().list(userId="me", q=query, maxResults=500).execute()
    messages.extend(result.get("messages", []))

    orders = []
    for msg_ref in messages:
        raw = service.users().messages().get(userId="me", id=msg_ref["id"], format="raw").execute()
        parsed = parse_zomato_email(raw)
        if parsed:
            orders.append(parsed)
    return orders


def parse_zomato_email(raw_message) -> Optional[dict]:
    try:
        msg_data = base64.urlsafe_b64decode(raw_message["raw"].encode("ASCII"))
        msg = email.message_from_bytes(msg_data, policy=policy.default)
        result = {"order_id": None, "restaurant": None, "date": msg["date"],
                  "items": [], "total_amount": None, "source": "zomato"}

        subject = msg.get("subject", "")
        if "Your Zomato order from " in subject:
            result["restaurant"] = subject.replace("Your Zomato order from ", "").strip()

        for part in msg.walk():
            if part.get_content_type() == "text/html":
                soup = BeautifulSoup(part.get_payload(decode=True), "html.parser")
                lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
                for line in lines:
                    if "ORDER ID:" in line:
                        result["order_id"] = line.replace("ORDER ID:", "").strip()
                    if " X " in line and line[0].isdigit():
                        result["items"].append(line)
                    if "Total paid" in line:
                        result["total_amount"] = line.replace("Total paid -", "").replace("Total paid", "").strip()
                break

        if not result["order_id"] or not result["items"]:
            return None

        result["items_str"] = " | ".join(result["items"])
        result["num_items"] = len(result["items"])
        return result
    except Exception:
        return None


def fetch_swiggy_orders(service) -> list[dict]:
    # Swiggy email parsing placeholder — extend as needed
    query = 'from:no-reply@swiggy.in subject:"Your order"'
    result = service.users().messages().list(userId="me", q=query, maxResults=200).execute()
    # TODO: parse Swiggy emails (structure differs from Zomato)
    return []


def estimate_plastic(restaurant: str, items_str: str) -> dict:
    try:
        response = azure_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Restaurant: {restaurant}\nItems: {items_str}"}
            ],
            temperature=0.1,
            max_tokens=300
        )
        return json.loads(response.choices[0].message.content.strip())
    except Exception as e:
        return {"restaurant_type": "unknown", "containers": 0, "lids": 0,
                "cutlery_pieces": 0, "outer_bags": 0, "plastic_covers": 0,
                "sauce_sachets": 0, "total_grams": 0, "reasoning": str(e)}


def compute_stats(orders: list[dict]) -> dict:
    df = pd.DataFrame(orders)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"])
    df["month"] = df["date"].dt.strftime("%Y-%m")

    total_g = float(df["total_grams"].sum())
    total_orders = len(df)

    # Monthly trend
    monthly = df.groupby("month")["total_grams"].sum().reset_index()
    monthly.columns = ["month", "plastic_grams"]

    # By restaurant type
    by_type = (df.groupby("restaurant_type")["total_grams"]
               .agg(["sum", "count", "mean"])
               .reset_index())
    by_type.columns = ["restaurant_type", "total_grams", "order_count", "avg_grams"]
    by_type = by_type.sort_values("total_grams", ascending=False)

    # Top offending restaurants
    top_restaurants = (df.groupby(["restaurant", "restaurant_type"])["total_grams"]
                       .agg(["sum", "count"])
                       .reset_index())
    top_restaurants.columns = ["restaurant", "restaurant_type", "total_grams", "order_count"]
    top_restaurants = top_restaurants.sort_values("total_grams", ascending=False).head(10)

    # Component breakdown
    components = {
        "containers": int(df["containers"].sum()),
        "lids": int(df["lids"].sum()),
        "cutlery_pieces": int(df["cutlery_pieces"].sum()),
        "outer_bags": int(df["outer_bags"].sum()),
        "plastic_covers": int(df.get("plastic_covers", pd.Series([0]*len(df))).sum()),
        "sauce_sachets": int(df["sauce_sachets"].sum()),
    }

    # Improvement suggestions
    worst_type = by_type.iloc[0]["restaurant_type"] if len(by_type) else "unknown"
    worst_type_avg = float(by_type.iloc[0]["avg_grams"]) if len(by_type) else 0
    best_type = by_type.iloc[-1]["restaurant_type"] if len(by_type) else "unknown"
    best_type_avg = float(by_type.iloc[-1]["avg_grams"]) if len(by_type) else 0

    return {
        "summary": {
            "total_grams": total_g,
            "total_kg": round(total_g / 1000, 2),
            "total_orders": total_orders,
            "avg_grams_per_order": round(total_g / total_orders, 1) if total_orders else 0,
            "date_from": str(df["date"].min().date()) if len(df) else None,
            "date_to": str(df["date"].max().date()) if len(df) else None,
        },
        "monthly_trend": monthly.to_dict(orient="records"),
        "by_restaurant_type": by_type.to_dict(orient="records"),
        "top_restaurants": top_restaurants.to_dict(orient="records"),
        "components": components,
        "insights": {
            "worst_category": worst_type,
            "worst_category_avg_g": worst_type_avg,
            "best_category": best_type,
            "best_category_avg_g": best_type_avg,
            "potential_saving_g": round((worst_type_avg - best_type_avg) *
                df[df["restaurant_type"] == worst_type].shape[0], 1)
        }
    }
