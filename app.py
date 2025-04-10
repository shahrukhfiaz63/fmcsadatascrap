from flask import Flask, request, jsonify
import requests
import io
import os
import re
import time
from PyPDF2 import PdfReader
from bs4 import BeautifulSoup

app = Flask(__name__)

def parse_pdf_for_mc(date):
    """
    Downloads and parses FMCSA PDF for the provided date (YYYYMMDD) to extract unique MC numbers.
    """
    pdf_url = f"https://li-public.fmcsa.dot.gov/lihtml/rptspdf/LI_REGISTER{date}.PDF"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/pdf",
        "Referer": "https://li-public.fmcsa.dot.gov/"
    }

    res = requests.get(pdf_url, headers=headers)
    if res.status_code != 200:
        raise ValueError(f"PDF not available. Status Code: {res.status_code}. URL: {pdf_url}")

    reader = PdfReader(io.BytesIO(res.content))
    full_text = ""
    for page in reader.pages:
        page_text = page.extract_text()
        if page_text:
            full_text += page_text + "\n"

    mc_numbers = []

    # Section 1: Between 'CERTIFICATES, PERMITS...' and 'CERTIFICATES OF REGISTRATION'
    pattern_1 = r"CERTIFICATES, PERMITS & LICENSES FILED AFTER JANUARY 1, 1995\s+NUMBER(.*?)CERTIFICATES OF REGISTRATION\s+NUMBER"
    match_1 = re.search(pattern_1, full_text, re.DOTALL | re.IGNORECASE)
    if match_1:
        text_1 = match_1.group(1)
        mc_numbers += re.findall(r'MC-\d{4,8}', text_1)

    # Section 2: Between 'CERTIFICATES OF REGISTRATION' and 'DISMISSALS'
    pattern_2 = r"CERTIFICATES OF REGISTRATION\s+NUMBER(.*?)DISMISSALS\s+Decisions"
    match_2 = re.search(pattern_2, full_text, re.DOTALL | re.IGNORECASE)
    if match_2:
        text_2 = match_2.group(1)
        mc_numbers += re.findall(r'MC-\d{4,8}', text_2)

    # Return unique sorted MC numbers
    return sorted(set(mc_numbers)), pdf_url

def fetch_usdot(mc):
    """
    Given an MC number, fetch the corresponding USDOT number from the FMCSA query.
    """
    url = f"https://safer.fmcsa.dot.gov/query.asp?searchtype=ANY&query_type=queryCarrierSnapshot&query_param=MC_MX&original_query_param=NAME&query_string={mc}"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://safer.fmcsa.dot.gov/",
        "Accept": "text/html,application/xhtml+xml"
    }

    try:
        res = requests.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            return {"error": f"FMCSA rejected the request for MC: {mc}", "status": res.status_code}

        match = re.search(r'USDOT Number:.*?(\d{4,8})', res.text)
        usdot = match.group(1) if match else None
        return {"mc": mc, "usdot": usdot}
    except Exception as e:
        return {"error": f"Exception while fetching USDOT for MC: {mc}", "details": str(e)}

def fetch_carrier_details(usdot):
    """
    Given a USDOT number, scrape the FMCSA carrier registration page to extract carrier details.
    """
    url = f"https://ai.fmcsa.dot.gov/SMS/Carrier/{usdot}/CarrierRegistration.aspx"
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://ai.fmcsa.dot.gov/",
        "Accept": "text/html"
    }

    try:
        res = requests.get(url, headers=headers, timeout=15)
        if res.status_code != 200:
            return {"error": f"Failed to fetch page for USDOT: {usdot}", "status": res.status_code}

        soup = BeautifulSoup(res.text, "html.parser")
        carrier_info = {}

        ul_col1 = soup.find("ul", class_="col1")
        if not ul_col1:
            return {"usdot": usdot, "carrier_info": {}, "message": "No carrier info found"}

        for li in ul_col1.find_all("li"):
            label = li.find("label")
            span = li.find("span", class_="dat")
            if label and span:
                key = label.get_text(strip=True).replace(":", "")
                value = span.get_text(separator=" ", strip=True).replace("\n", " ").replace("\r", "")
                carrier_info[key] = value

        return {"usdot": usdot, "carrier_info": carrier_info}

    except Exception as e:
        return {"error": f"Exception during scraping for USDOT: {usdot}", "details": str(e)}

@app.route("/result", methods=["GET"])
def result():
    # Get the date parameter
    date = request.args.get('date')
    if not date:
        return jsonify({"error": "Missing date parameter in format YYYYMMDD"}), 400

    try:
        mc_numbers, pdf_url = parse_pdf_for_mc(date)
    except Exception as e:
        return jsonify({"error": "Failed to parse PDF", "details": str(e)}), 400

    results = []

    # Process each MC number:
    for mc in mc_numbers:
        # Fetch USDOT corresponding to the MC number
        usdot_result = fetch_usdot(mc)
        usdot = usdot_result.get("usdot")
        if not usdot:
            # If USDOT is not found or an error occurred, record the error for this MC
            results.append({
                "mc": mc,
                "usdot": None,
                "carrier_details": {"error": "USDOT not found or error occurred", "details": usdot_result.get("error", "")}
            })
            continue

        # Wait 3 seconds before fetching carrier details (only for carrier details fetch)
        time.sleep(3)

        # Fetch carrier details for the USDOT number
        carrier_detail = fetch_carrier_details(usdot)
        results.append({
            "mc": mc,
            "usdot": usdot,
            "carrier_details": carrier_detail
        })

    return jsonify({
        "date": date,
        "pdf_url": pdf_url,
        "total_mc_numbers": len(mc_numbers),
        "results": results
    })

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
