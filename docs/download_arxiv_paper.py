import arxiv
import os
import json
import requests
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================
# CONFIG
# ==========================
QUERY = "cat:cs.AI OR cat:cs.LG OR cat:cs.CL OR cat:stat.ML"
MAX_RESULTS = 1000
MAX_WORKERS = 6   # SAFE range: 4–8

BASE_DIR = "arxiv_data"
PDF_DIR = os.path.join(BASE_DIR, "pdfs")
META_FILE = os.path.join(BASE_DIR, "metadata.json")

os.makedirs(PDF_DIR, exist_ok=True)

# ==========================
# STEP 1: FETCH METADATA FAST
# ==========================
search = arxiv.Search(
    query=QUERY,
    max_results=MAX_RESULTS,
    sort_by=arxiv.SortCriterion.SubmittedDate
)

client = arxiv.Client(page_size=200)

results = list(client.results(search))

metadata = []

for i, r in enumerate(results):
    metadata.append({
        "index": i,
        "id": r.entry_id,
        "title": r.title,
        "authors": [a.name for a in r.authors],
        "summary": r.summary,
        "pdf_url": r.pdf_url,
        "categories": r.categories,
    })

with open(META_FILE, "w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=2)

print(f"Metadata collected: {len(metadata)} papers")

# ==========================
# STEP 2: MULTITHREADED PDF DOWNLOAD
# ==========================
def download_pdf(paper):
    try:
        filename = f"paper_{paper['index']:04d}.pdf"
        path = os.path.join(PDF_DIR, filename)

        if os.path.exists(path):
            return "skipped"

        response = requests.get(paper["pdf_url"], timeout=20)
        response.raise_for_status()

        with open(path, "wb") as f:
            f.write(response.content)

        return "downloaded"

    except Exception as e:
        return f"error: {e}"

# --------------------------
# Execute downloads
# --------------------------
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    futures = [executor.submit(download_pdf, p) for p in metadata]

    for _ in tqdm(as_completed(futures), total=len(futures)):
        pass

print("All PDFs downloaded")
