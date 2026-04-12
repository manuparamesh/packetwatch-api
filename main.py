"""
PacketWatch FastAPI Backend v4
Food delivery + Ecommerce plastic estimation
Supabase persistent storage
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import uuid, os, json, base64, email, httpx
from email import policy as email_policy
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
    description="Estimate plastic waste from food delivery and ecommerce orders.",
    version="4.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")

# ── PLATFORM CONFIG ───────────────────────────────────────────────────────────
PLATFORMS = {
    # ── INDIA FOOD ──
    "zomato":     {"sender":"noreply@zomato.com",              "subject":"Your Zomato order from",               "country_code":"IN", "type":"food",      "plastic_modifier":1.0,  "parse_fn":"zomato"},
    "swiggy":     {"sender":"noreply@swiggy.in",               "subject":"Your Swiggy order was successfully",   "country_code":"IN", "type":"food",      "plastic_modifier":1.0,  "parse_fn":"swiggy"},
    # ── INDIA ECOMMERCE ──
    "amazon_in":  {"sender":"shipment-tracking@amazon.in",     "subject":"Your Amazon.in order",                 "country_code":"IN", "type":"ecommerce", "plastic_modifier":1.1,  "parse_fn":"amazon"},
    "flipkart":   {"sender":"noreply@flipkart.com",            "subject":"Your order has been",                  "country_code":"IN", "type":"ecommerce", "plastic_modifier":1.05, "parse_fn":"ecommerce"},
    # ── USA FOOD ──
    "doordash":   {"sender":"no-reply@doordash.com",           "subject":"Your DoorDash order",                  "country_code":"US", "type":"food",      "plastic_modifier":0.8,  "parse_fn":"generic"},
    "uber_eats":  {"sender":"uber.eats@uber.com",              "subject":"Your Uber Eats order",                 "country_code":"US", "type":"food",      "plastic_modifier":0.75, "parse_fn":"generic"},
    # ── USA ECOMMERCE ──
    "amazon_us":  {"sender":"shipment-tracking@amazon.com",    "subject":"Your Amazon.com order",                "country_code":"US", "type":"ecommerce", "plastic_modifier":0.9,  "parse_fn":"amazon"},
    "walmart":    {"sender":"help@walmart.com",                "subject":"Your Walmart order",                   "country_code":"US", "type":"ecommerce", "plastic_modifier":0.85, "parse_fn":"ecommerce"},
    # ── UK FOOD ──
    "deliveroo":  {"sender":"no-reply@deliveroo.co.uk",        "subject":"Your Deliveroo order",                 "country_code":"GB", "type":"food",      "plastic_modifier":0.65, "parse_fn":"generic"},
    "uber_eats_gb":{"sender":"uber.eats@uber.com",             "subject":"Your Uber Eats order",                 "country_code":"GB", "type":"food",      "plastic_modifier":0.65, "parse_fn":"generic"},
    # ── UK ECOMMERCE ──
    "amazon_uk":  {"sender":"shipment-tracking@amazon.co.uk",  "subject":"Your Amazon.co.uk order",              "country_code":"GB", "type":"ecommerce", "plastic_modifier":0.8,  "parse_fn":"amazon"},
    "asos":       {"sender":"noreply@asos.com",                "subject":"Your ASOS order",                      "country_code":"GB", "type":"ecommerce", "plastic_modifier":0.75, "parse_fn":"ecommerce"},
    # ── EUROPE FOOD ──
    "bolt_food":  {"sender":"food@bolt.eu",                    "subject":"Your Bolt Food order",                 "country_code":"EU", "type":"food",      "plastic_modifier":0.7,  "parse_fn":"generic"},
    "deliveroo_eu":{"sender":"no-reply@deliveroo.fr",          "subject":"Your Deliveroo order",                 "country_code":"EU", "type":"food",      "plastic_modifier":0.65, "parse_fn":"generic"},
    # ── EUROPE ECOMMERCE ──
    "amazon_eu":  {"sender":"shipment-tracking@amazon.de",     "subject":"Your Amazon order",                    "country_code":"EU", "type":"ecommerce", "plastic_modifier":0.75, "parse_fn":"amazon"},
    "zalando":    {"sender":"orders@zalando.com",              "subject":"Your Zalando order",                   "country_code":"EU", "type":"ecommerce", "plastic_modifier":0.7,  "parse_fn":"ecommerce"},
    # ── MIDDLE EAST FOOD ──
    "talabat":    {"sender":"noreply@talabat.com",             "subject":"Your talabat order",                   "country_code":"AE", "type":"food",      "plastic_modifier":1.1,  "parse_fn":"generic"},
    "uber_eats_ae":{"sender":"uber.eats@uber.com",             "subject":"Your Uber Eats order",                 "country_code":"AE", "type":"food",      "plastic_modifier":1.0,  "parse_fn":"generic"},
    # ── MIDDLE EAST ECOMMERCE ──
    "amazon_ae":  {"sender":"shipment-tracking@amazon.ae",     "subject":"Your Amazon.ae order",                 "country_code":"AE", "type":"ecommerce", "plastic_modifier":1.1,  "parse_fn":"amazon"},
    "noon":       {"sender":"noreply@noon.com",                "subject":"Your noon order",                      "country_code":"AE", "type":"ecommerce", "plastic_modifier":1.05, "parse_fn":"ecommerce"},
    # ── SE ASIA FOOD ──
    "grab_food":  {"sender":"no-reply@grab.com",               "subject":"Your GrabFood order",                  "country_code":"SG", "type":"food",      "plastic_modifier":1.05, "parse_fn":"generic"},
    "uber_eats_sg":{"sender":"uber.eats@uber.com",             "subject":"Your Uber Eats order",                 "country_code":"SG", "type":"food",      "plastic_modifier":1.0,  "parse_fn":"generic"},
    # ── SE ASIA ECOMMERCE ──
    "shopee":     {"sender":"noreply@shopee.com",              "subject":"Your Shopee order",                    "country_code":"SG", "type":"ecommerce", "plastic_modifier":1.1,  "parse_fn":"ecommerce"},
    "lazada":     {"sender":"noreply@lazada.com",              "subject":"Your Lazada order",                    "country_code":"SG", "type":"ecommerce", "plastic_modifier":1.05, "parse_fn":"ecommerce"},
}

# ── FOOD SYSTEM PROMPT ────────────────────────────────────────────────────────
FOOD_PROMPT = """
You are a packaging waste expert for food delivery worldwide.
Estimate PLASTIC waste only — not paper, cardboard, or foil.

Given restaurant name, items ordered, and region, return JSON:
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

restaurant_type must be one of: "biryani","burger_fast_food","bakery_dessert","south_indian",
"cafe_coffee","roll_wrap_shawarma","dhaba_northindian","healthy_bowl","sandwich_sub",
"pizza","chinese_asian","seafood","multicuisine","juice_beverage","ice_cream","breakfast",
"middle_eastern","western_casual"

WEIGHTS: container=15g, lid=5g, cutlery=3g, bag=8g, cover=4g, sachet=2g

RULES:
1. Indian flat breads/Dosa/Idli packed together: covers=1, containers=0
2. Indian biryani: add covers=2 for raita+pickle per portion
3. Burger/fast food chains: paper packaging, containers=0, bags=0
4. Rolls/Wraps/Shawarma: foil/paper, containers=0
5. Cakes/Pastries: cardboard, containers=0
6. Ice cream scoops: containers=1+lid per item
7. US/EU restaurants: lower sachet count than Indian/SEA
8. Middle Eastern/SEA: high sachet count (+2 per main dish)
9. outer_bags=1 for most orders; 0 for burger/sandwich chains
10. Multiple same flat item: 1 cover total not N containers

Return ONLY valid JSON. No markdown.
"""

# ── ECOMMERCE SYSTEM PROMPT ───────────────────────────────────────────────────
ECOMMERCE_PROMPT = """
You are a packaging waste expert specialising in ecommerce plastic waste.
Estimate PLASTIC waste only — not paper, cardboard, or air pillows.

Given the platform, product names, and region, return JSON:
{
  "product_category": <string>,
  "bubble_wrap_sheets": <int>,
  "plastic_bags": <int>,
  "plastic_tape_meters": <float>,
  "foam_pieces": <int>,
  "plastic_film_wraps": <int>,
  "total_grams": <float>,
  "reasoning": <string>
}

product_category must be one of: "electronics","clothing_fashion","books_stationery",
"home_appliances","kitchen","beauty_personal","toys_games","sports_fitness",
"grocery_food","furniture","jewellery","mobile_accessories","mixed"

WEIGHTS: bubble_wrap_sheet=8g, plastic_bag=12g, tape_per_meter=5g,
foam_piece=10g, plastic_film_wrap=6g

ECOMMERCE PACKAGING RULES:
1. Electronics: bubble wrap + plastic bag + tape. Mobile phones get extra foam.
2. Clothing/Fashion: plastic poly bag only (no box usually). 1 bag per item.
3. Books: minimal plastic. Usually just tape on cardboard. 1-2g plastic.
4. Large appliances (washing machine, TV, fridge): heavy plastic film + foam. 200-400g.
5. Small appliances (mixer, kettle): bubble wrap + plastic bag. 40-80g.
6. Beauty products: plastic film wrap. Small items often in bubble mailer (plastic).
7. Toys: often in plastic bags inside cardboard. 20-50g.
8. Grocery/food items: plastic bags + sealing. 20-40g.
9. Jewellery: small plastic pouch + bubble wrap. 10-20g.
10. Amazon uses air pillows (plastic) heavily — add 2-4 air pillows (5g each) per order.
11. Flipkart/Shopee/Lazada use plastic outer wrap on cardboard boxes.
12. Fashion platforms (ASOS, Zalando) use poly mailer bags — 1 per order, 15-25g.
13. Scale plastic by number of items but not linearly — shared packaging reduces per-item plastic.

Return ONLY valid JSON. No markdown.
"""

# ── SUPABASE ──────────────────────────────────────────────────────────────────
async def supabase_insert(record: dict):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/analyses",
                headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                         "Content-Type": "application/json", "Prefer": "return=minimal"},
                json=record, timeout=10
            )
    except Exception as e:
        print(f"Supabase insert error: {e}")

async def supabase_get_stats() -> dict:
    if not SUPABASE_URL or not SUPABASE_KEY:
        return {"total_users": 0, "total_kg": 0, "by_country": []}
    try:
        async with httpx.AsyncClient() as client:
            res = await client.get(
                f"{SUPABASE_URL}/rest/v1/analyses?select=country,total_kg,total_orders,platform,created_at",
                headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
                timeout=10
            )
            rows = res.json()
            if not isinstance(rows, list):
                return {"total_users": 0, "total_kg": 0, "by_country": []}
            total_users = len(rows)
            total_kg = round(sum(r.get("total_kg", 0) for r in rows), 2)
            by_country: dict = {}
            for r in rows:
                c = r.get("country", "IN")
                if c not in by_country:
                    by_country[c] = {"country": c, "users": 0, "total_kg": 0}
                by_country[c]["users"] += 1
                by_country[c]["total_kg"] = round(by_country[c]["total_kg"] + r.get("total_kg", 0), 2)
            return {"total_users": total_users, "total_kg": total_kg,
                    "total_orders": sum(r.get("total_orders", 0) for r in rows),
                    "by_country": list(by_country.values())}
    except Exception as e:
        print(f"Supabase fetch error: {e}")
        return {"total_users": 0, "total_kg": 0, "by_country": []}

async def supabase_feedback_insert(record: dict):
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"{SUPABASE_URL}/rest/v1/feedback",
                headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
                         "Content-Type": "application/json", "Prefer": "return=minimal"},
                json=record, timeout=10
            )
    except Exception as e:
        print(f"Supabase feedback error: {e}")

# ── ROUTES ────────────────────────────────────────────────────────────────────
@app.get("/")
def root():
    return {"status": "PacketWatch API v4.0 running", "docs": "/docs"}

@app.get("/global-stats")
async def get_global_stats():
    return await supabase_get_stats()

@app.get("/auth/url")
def get_auth_url(redirect_uri: Optional[str] = None):
    uri = redirect_uri or os.getenv("REDIRECT_URI")
    config = json.loads(json.dumps(GOOGLE_CLIENT_CONFIG))
    config["web"]["redirect_uris"] = [uri]
    flow = Flow.from_client_config(config, scopes=SCOPES)
    flow.redirect_uri = uri
    auth_url, state = flow.authorization_url(access_type="offline", include_granted_scopes="true")
    return {"auth_url": auth_url, "state": state}

class CallbackRequest(BaseModel):
    code: str
    state: str
    apps: list[str] = ["zomato"]
    country: Optional[str] = "IN"
    redirect_uri: Optional[str] = None
    max_orders: Optional[int] = 10

@app.post("/auth/callback")
def auth_callback(req: CallbackRequest, background_tasks: BackgroundTasks):
    uri = req.redirect_uri or os.getenv("REDIRECT_URI")
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
    jobs[job_id] = {"status": "processing", "progress": 0, "result": None, "message": "Starting..."}
    background_tasks.add_task(run_pipeline, job_id, creds, req.apps, req.country, req.max_orders or 10)
    return {"job_id": job_id}

@app.get("/job/{job_id}")
def get_job_status(job_id: str):
    if job_id not in jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    job = jobs[job_id]
    return {"status": job["status"], "progress": job.get("progress", 0),
            "message": job.get("message", ""),
            "result": job["result"] if job["status"] == "done" else None}

class FeedbackRequest(BaseModel):
    message: str
    country: Optional[str] = "IN"
    total_kg: Optional[float] = None
    rating: Optional[int] = None

@app.post("/feedback")
async def submit_feedback(req: FeedbackRequest):
    if not req.message or len(req.message.strip()) < 3:
        raise HTTPException(status_code=400, detail="Feedback too short")
    await supabase_feedback_insert({
        "message": req.message.strip(),
        "country": req.country,
        "total_kg": req.total_kg,
        "rating": req.rating,
    })
    return {"status": "thanks"}

# ── PIPELINE ──────────────────────────────────────────────────────────────────
def run_pipeline(job_id: str, creds: Credentials, apps: list[str], country: str, max_orders: int):
    import asyncio
    try:
        service = build("gmail", "v1", credentials=creds)
        all_orders = []

        for app_id in apps:
            if app_id not in PLATFORMS:
                continue
            platform = PLATFORMS[app_id]
            jobs[job_id]["message"] = f"Fetching {app_id.replace('_',' ').title()} emails..."

            parse_fn = platform["parse_fn"]
            if parse_fn == "zomato":
                orders = fetch_zomato_orders(service, max_orders)
            elif parse_fn == "swiggy":
                orders = fetch_swiggy_orders(service, max_orders)
            elif parse_fn == "amazon":
                orders = fetch_amazon_orders(service, platform, max_orders)
            else:
                orders = fetch_generic_orders(service, platform, max_orders)

            for o in orders:
                o["platform"] = app_id
                o["platform_type"] = platform["type"]
                o["country_code"] = platform["country_code"]
                o["plastic_modifier"] = platform["plastic_modifier"]
            all_orders.extend(orders)

        all_orders = all_orders[:max_orders]

        if not all_orders:
            jobs[job_id] = {
                "status": "done", "progress": 100,
                "result": {"error": "no_orders", "summary": {"total_orders": 0}},
                "message": "No orders found"
            }
            return

        results = []
        for i, order in enumerate(all_orders):
            jobs[job_id]["message"] = f"Analysing order {i+1} of {len(all_orders)}..."
            platform_type = order.get("platform_type", "food")
            if platform_type == "ecommerce":
                estimate = estimate_ecommerce_plastic(
                    order.get("platform", ""), order["items_str"], order.get("country_code", "IN")
                )
            else:
                estimate = estimate_food_plastic(
                    order["restaurant"], order["items_str"], order.get("country_code", "IN")
                )
            modifier = order.get("plastic_modifier", 1.0)
            estimate["total_grams"] = round(estimate["total_grams"] * modifier, 1)
            order.update(estimate)
            results.append(order)
            jobs[job_id]["progress"] = int((i + 1) / len(all_orders) * 100)

        jobs[job_id]["message"] = "Building your report..."
        stats = compute_stats(results, country)

        asyncio.run(supabase_insert({
            "country": country,
            "total_kg": stats["summary"]["total_kg"],
            "total_orders": stats["summary"]["total_orders"],
            "platform": ",".join(apps),
        }))

        jobs[job_id] = {"status": "done", "progress": 100, "result": stats, "message": "Done!"}

    except Exception as e:
        jobs[job_id] = {"status": "error", "message": str(e), "result": None, "progress": 0}

# ── EMAIL PARSERS ─────────────────────────────────────────────────────────────
def fetch_zomato_orders(service, max_orders=10):
    query = 'from:noreply@zomato.com subject:"Your Zomato order from"'
    result = service.users().messages().list(userId="me", q=query, maxResults=max_orders).execute()
    messages = result.get("messages", [])[:max_orders]
    orders = []
    for msg_ref in messages:
        raw = service.users().messages().get(userId="me", id=msg_ref["id"], format="raw").execute()
        parsed = parse_zomato_email(raw)
        if parsed:
            orders.append(parsed)
        if len(orders) >= max_orders:
            break
    return orders

def parse_zomato_email(raw_message) -> Optional[dict]:
    try:
        msg_data = base64.urlsafe_b64decode(raw_message["raw"].encode("ASCII"))
        msg = email.message_from_bytes(msg_data, policy=email_policy.default)
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
                    if " X " in line and len(line) > 0 and line[0].isdigit():
                        result["items"].append(line)
                    if "Total paid" in line:
                        result["total_amount"] = line.replace("Total paid -", "").replace("Total paid", "").strip()
                break
        if not result["restaurant"] or not result["items"]:
            return None
        if not result["order_id"]:
            result["order_id"] = f"z-{result['date']}"
        result["items_str"] = " | ".join(result["items"])
        result["num_items"] = len(result["items"])
        return result
    except Exception:
        return None

def fetch_swiggy_orders(service, max_orders=10):
    query = 'from:noreply@swiggy.in subject:"Your Swiggy order was successfully"'
    result = service.users().messages().list(userId="me", q=query, maxResults=max_orders).execute()
    messages = result.get("messages", [])[:max_orders]
    orders = []
    for msg_ref in messages:
        raw = service.users().messages().get(userId="me", id=msg_ref["id"], format="raw").execute()
        parsed = parse_swiggy_email(raw)
        if parsed:
            orders.append(parsed)
        if len(orders) >= max_orders:
            break
    return orders

def parse_swiggy_email(raw_message) -> Optional[dict]:
    try:
        msg_data = base64.urlsafe_b64decode(raw_message["raw"].encode("ASCII"))
        msg = email.message_from_bytes(msg_data, policy=email_policy.default)
        result = {"order_id": None, "restaurant": None, "date": msg["date"],
                  "items": [], "total_amount": None, "source": "swiggy"}
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                soup = BeautifulSoup(part.get_payload(decode=True), "html.parser")
                text = soup.get_text("\n", strip=True)
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                for i, line in enumerate(lines):
                    if "Order No" in line:
                        candidate = line.replace("Order No:", "").replace("Order No", "").strip()
                        if candidate and candidate[0].isdigit():
                            result["order_id"] = candidate
                        elif i + 1 < len(lines) and lines[i+1][0].isdigit():
                            result["order_id"] = lines[i+1].strip()
                    if line.lower().startswith("ordered from") and i + 1 < len(lines):
                        result["restaurant"] = lines[i+1].strip()
                    if "Total" in line and "₹" in line:
                        result["total_amount"] = line.strip()
                import re
                for line in lines:
                    if line[0:1].isdigit() and (" x " in line.lower() or " X " in line):
                        result["items"].append(line)
                    elif re.match(r'.+\s+[xX]\s+\d+$', line):
                        parts = re.split(r'\s+[xX]\s+', line)
                        if len(parts) == 2:
                            result["items"].append(f"{parts[1]} X {parts[0]}")
                break
        if not result["restaurant"]:
            return None
        if not result["items"]:
            result["items"] = [f"1 X Order from {result['restaurant']}"]
        if not result["order_id"]:
            result["order_id"] = f"sw-{result['date']}"
        result["items_str"] = " | ".join(result["items"])
        result["num_items"] = len(result["items"])
        return result
    except Exception as e:
        print(f"Swiggy parse error: {e}")
        return None

def fetch_amazon_orders(service, platform: dict, max_orders=10):
    query = f'from:{platform["sender"]} subject:"{platform["subject"]}"'
    result = service.users().messages().list(userId="me", q=query, maxResults=max_orders).execute()
    messages = result.get("messages", [])[:max_orders]
    orders = []
    for msg_ref in messages:
        raw = service.users().messages().get(userId="me", id=msg_ref["id"], format="raw").execute()
        parsed = parse_amazon_email(raw, platform)
        if parsed:
            orders.append(parsed)
        if len(orders) >= max_orders:
            break
    return orders

def parse_amazon_email(raw_message, platform: dict) -> Optional[dict]:
    try:
        msg_data = base64.urlsafe_b64decode(raw_message["raw"].encode("ASCII"))
        msg = email.message_from_bytes(msg_data, policy=email_policy.default)
        result = {"order_id": f"amz-{uuid.uuid4().hex[:8]}", "restaurant": platform.get("sender","amazon").split("@")[-1].split(".")[0].title(),
                  "date": msg["date"], "items": [], "total_amount": None,
                  "source": "amazon", "platform_type": "ecommerce"}
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                soup = BeautifulSoup(part.get_payload(decode=True), "html.parser")
                text = soup.get_text("\n", strip=True)
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                import re
                for i, line in enumerate(lines):
                    if re.match(r'\d{3}-\d{7}-\d{7}', line):
                        result["order_id"] = line.strip()
                    if "Order #" in line or "Order ID" in line:
                        result["order_id"] = line.replace("Order #","").replace("Order ID","").strip()
                    if len(line) > 15 and len(line) < 150 and not any(w in line.lower() for w in
                        ["amazon","delivery","shipped","order","track","address","payment","total","hello","dear"]):
                        result["items"].append(line)
                break
        if not result["items"]:
            result["items"] = ["1 X Online order item"]
        result["items"] = result["items"][:5]
        result["items_str"] = " | ".join(result["items"])
        result["num_items"] = len(result["items"])
        return result
    except Exception:
        return None

def fetch_generic_orders(service, platform: dict, max_orders=10):
    query = f'from:{platform["sender"]} subject:"{platform["subject"]}"'
    result = service.users().messages().list(userId="me", q=query, maxResults=max_orders).execute()
    messages = result.get("messages", [])[:max_orders]
    orders = []
    for msg_ref in messages:
        raw = service.users().messages().get(userId="me", id=msg_ref["id"], format="raw").execute()
        parsed = parse_generic_email(raw, platform)
        if parsed:
            orders.append(parsed)
        if len(orders) >= max_orders:
            break
    return orders

def parse_generic_email(raw_message, platform: dict) -> Optional[dict]:
    try:
        msg_data = base64.urlsafe_b64decode(raw_message["raw"].encode("ASCII"))
        msg = email.message_from_bytes(msg_data, policy=email_policy.default)
        result = {"order_id": f"order-{uuid.uuid4().hex[:8]}", "restaurant": "Unknown",
                  "date": msg["date"], "items": ["1 X Main dish"], "total_amount": None,
                  "source": platform.get("parse_fn", "unknown")}
        subject = msg.get("subject", "")
        for pattern in ["from ", "at ", "From "]:
            if pattern in subject:
                result["restaurant"] = subject.split(pattern)[-1].strip()
                break
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                soup = BeautifulSoup(part.get_payload(decode=True), "html.parser")
                lines = [l.strip() for l in soup.get_text("\n", strip=True).split("\n") if l.strip()]
                items = [l for l in lines if any(p in l.lower() for p in [" x ", "qty:", "×"]) and len(l) < 80]
                if items:
                    result["items"] = items[:5]
                break
        result["items_str"] = " | ".join(result["items"])
        result["num_items"] = len(result["items"])
        return result
    except Exception:
        return None

# ── ESTIMATORS ────────────────────────────────────────────────────────────────
def estimate_food_plastic(restaurant: str, items_str: str, country_code: str = "IN") -> dict:
    region = {"IN":"India","US":"USA","GB":"UK","EU":"Europe","AE":"Middle East","SG":"Southeast Asia"}.get(country_code,"India")
    try:
        response = azure_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role":"system","content":FOOD_PROMPT},
                      {"role":"user","content":f"Region: {region}\nRestaurant: {restaurant}\nItems: {items_str}"}],
            temperature=0.1, max_tokens=300
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw.strip())
        data["platform_type"] = "food"
        return data
    except Exception as e:
        return {"restaurant_type":"multicuisine","containers":1,"lids":1,"cutlery_pieces":1,
                "outer_bags":1,"plastic_covers":0,"sauce_sachets":1,"total_grams":38.0,
                "reasoning":str(e),"platform_type":"food"}

def estimate_ecommerce_plastic(platform: str, items_str: str, country_code: str = "IN") -> dict:
    region = {"IN":"India","US":"USA","GB":"UK","EU":"Europe","AE":"Middle East","SG":"Southeast Asia"}.get(country_code,"India")
    try:
        response = azure_client.chat.completions.create(
            model=DEPLOYMENT,
            messages=[{"role":"system","content":ECOMMERCE_PROMPT},
                      {"role":"user","content":f"Region: {region}\nPlatform: {platform}\nProducts ordered: {items_str}"}],
            temperature=0.1, max_tokens=300
        )
        raw = response.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw.strip())
        data["platform_type"] = "ecommerce"
        # Normalise to common field name
        if "total_grams" not in data:
            data["total_grams"] = 50.0
        return data
    except Exception as e:
        return {"product_category":"mixed","bubble_wrap_sheets":1,"plastic_bags":1,
                "plastic_tape_meters":0.5,"foam_pieces":0,"plastic_film_wraps":1,
                "total_grams":50.0,"reasoning":str(e),"platform_type":"ecommerce"}

# ── STATS ─────────────────────────────────────────────────────────────────────
def compute_stats(orders: list[dict], country: str = "IN") -> dict:
    df = pd.DataFrame(orders)
    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    df["month"] = df["date"].dt.strftime("%Y-%m")

    total_g = float(df["total_grams"].sum())
    total_orders = len(df)

    monthly = df.groupby("month")["total_grams"].sum().reset_index()
    monthly.columns = ["month", "plastic_grams"]

    # Safely check platform_type column
    has_platform_type = "platform_type" in df.columns
    food_orders = df[df["platform_type"] == "food"] if has_platform_type else df
    ecom_orders = df[df["platform_type"] == "ecommerce"] if has_platform_type else pd.DataFrame()

    # Use restaurant_type if available, else platform_type, else skip
    if "restaurant_type" in df.columns:
        by_type_col = "restaurant_type"
    elif has_platform_type:
        by_type_col = "platform_type"
    else:
        by_type_col = None

    if by_type_col:
        by_type = df.groupby(by_type_col)["total_grams"].agg(["sum","count","mean"]).reset_index()
        by_type.columns = ["restaurant_type","total_grams","order_count","avg_grams"]
        by_type = by_type.sort_values("total_grams", ascending=False)
    else:
        by_type = pd.DataFrame(columns=["restaurant_type","total_grams","order_count","avg_grams"])

    top_restaurants = df.groupby(["restaurant"])["total_grams"].agg(["sum","count"]).reset_index()
    top_restaurants.columns = ["restaurant","total_grams","order_count"]
    if has_platform_type:
        top_restaurants["restaurant_type"] = df.groupby("restaurant")["platform_type"].first().values
    else:
        top_restaurants["restaurant_type"] = "food"
    top_restaurants = top_restaurants.sort_values("total_grams", ascending=False).head(10)

    def safe_sum(col):
        return int(df[col].sum()) if col in df.columns else 0

    per_order_cols = ["date","restaurant","total_grams","items_str"]
    per_order = df[per_order_cols].copy()
    if has_platform_type:
        per_order["platform_type"] = df["platform_type"]
    else:
        per_order["platform_type"] = "food"
    if "restaurant_type" in df.columns:
        per_order["restaurant_type"] = df["restaurant_type"]
    per_order["date"] = per_order["date"].dt.strftime("%b %d, %Y")
    per_order = per_order.fillna("")

    worst_type = by_type.iloc[0]["restaurant_type"] if len(by_type) else "unknown"
    worst_avg = float(by_type.iloc[0]["avg_grams"]) if len(by_type) else 0
    best_type = by_type.iloc[-1]["restaurant_type"] if len(by_type) else "unknown"
    best_avg = float(by_type.iloc[-1]["avg_grams"]) if len(by_type) else 0

    if by_type_col and len(by_type):
        worst_count = df[df[by_type_col] == worst_type].shape[0]
        potential_saving = round((worst_avg - best_avg) * worst_count, 1)
    else:
        potential_saving = 0

    return {
        "summary": {
            "total_grams": total_g,
            "total_kg": round(total_g/1000, 3),
            "total_orders": total_orders,
            "food_orders": len(food_orders),
            "ecom_orders": len(ecom_orders),
            "avg_grams_per_order": round(total_g/total_orders, 1) if total_orders else 0,
            "date_from": str(df["date"].min().date()) if len(df) else None,
            "date_to": str(df["date"].max().date()) if len(df) else None,
            "country": country,
        },
        "per_order": per_order.to_dict(orient="records"),
        "monthly_trend": monthly.to_dict(orient="records"),
        "by_restaurant_type": by_type.to_dict(orient="records"),
        "top_restaurants": top_restaurants.to_dict(orient="records"),
        "components": {
            "containers": safe_sum("containers"),
            "lids": safe_sum("lids"),
            "cutlery_pieces": safe_sum("cutlery_pieces"),
            "outer_bags": safe_sum("outer_bags"),
            "plastic_covers": safe_sum("plastic_covers"),
            "sauce_sachets": safe_sum("sauce_sachets"),
            "bubble_wrap_sheets": safe_sum("bubble_wrap_sheets"),
            "plastic_bags": safe_sum("plastic_bags"),
            "plastic_film_wraps": safe_sum("plastic_film_wraps"),
        },
        "insights": {
            "worst_category": worst_type,
            "worst_category_avg_g": worst_avg,
            "best_category": best_type,
            "best_category_avg_g": best_avg,
            "potential_saving_g": potential_saving
        }
    }
