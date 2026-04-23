import os
import time
import glob
import json
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import (
    NoSuchElementException,
    TimeoutException,
    WebDriverException,
    StaleElementReferenceException,
    NoAlertPresentException,
)
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(BASE_DIR, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
_log_file = os.path.join(LOG_DIR, f"dispatch_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(_log_file, encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)
log.info("Log file: %s", _log_file)

DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads_dispatch_summary")
FINAL_FILENAME = "Dispatch_summary.csv"


def get_date_batches():
    """Returns 3 (start, end) date pairs of 10 days each covering the last 30 days, oldest first."""
    today = datetime.now().date()
    batches = []
    for i in range(3):
        end = today - timedelta(days=i * 10)
        start = today - timedelta(days=(i + 1) * 10 - 1)
        batches.append((start, end))
    return list(reversed(batches))


# ──────────────────────────────────────────────
#  Chrome / Selenium helpers
# ──────────────────────────────────────────────

def setup_driver(download_dir):
    os.makedirs(download_dir, exist_ok=True)
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-notifications")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--remote-debugging-port=9222")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_experimental_option("prefs", {
        "download.default_directory": download_dir,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True,
    })
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    # Enable downloads in headless mode
    driver.execute_cdp_cmd("Page.setDownloadBehavior", {
        "behavior": "allow",
        "downloadPath": download_dir,
    })
    return driver


def find_element_across_iframes(driver, selector, by=By.CSS_SELECTOR, timeout=20):
    deadline = time.time() + timeout
    while time.time() < deadline:
        driver.switch_to.default_content()
        try:
            return driver.find_element(by, selector)
        except NoSuchElementException:
            pass
        iframes = driver.find_elements(By.TAG_NAME, "iframe")
        for frame in iframes:
            driver.switch_to.default_content()
            try:
                driver.switch_to.frame(frame)
                return driver.find_element(by, selector)
            except (NoSuchElementException, WebDriverException):
                continue
        time.sleep(1)
    driver.switch_to.default_content()
    raise TimeoutException(f"Element not found: {by}='{selector}' within {timeout}s")


def js_click(driver, element):
    driver.execute_script("arguments[0].click();", element)


def wait_for_download(download_dir, started_after, timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        files = glob.glob(os.path.join(download_dir, "*"))
        candidates = [
            f for f in files
            if not f.endswith(".crdownload")
            and not f.endswith(".tmp")
            and os.path.getctime(f) > started_after
        ]
        if candidates:
            return max(candidates, key=os.path.getctime)
        time.sleep(2)
    return None


# ──────────────────────────────────────────────
#  Vinculum login
# ──────────────────────────────────────────────

def vinculum_login(driver):
    wait = WebDriverWait(driver, 20)
    login_url = os.getenv("VINCULUM_LOGIN_URL")
    username = os.getenv("VINCULUM_USERNAME")
    password = os.getenv("VINCULUM_PASSWORD")

    log.info("Opening Vinculum login page...")
    driver.get(login_url)

    wait.until(EC.element_to_be_clickable((By.ID, "userName"))).send_keys(username)
    driver.find_element(By.ID, "password").send_keys(password)
    driver.find_element(By.NAME, "Login").click()
    time.sleep(2)

    try:
        alert = driver.switch_to.alert
        log.info("Alert detected: '%s' — accepting.", alert.text)
        alert.accept()
        time.sleep(2)
    except NoAlertPresentException:
        pass

    log.info("Waiting for dashboard to load after login...")
    time.sleep(15)
    log.info("Current URL: %s", driver.current_url)
    driver.save_screenshot(os.path.join(DOWNLOAD_DIR, "post_login.png"))
    log.info("Screenshot saved: post_login.png")
    WebDriverWait(driver, 120).until(
        EC.presence_of_element_located((By.XPATH, "//a[.//span[contains(text(), 'WMS')]]"))
    )
    log.info("Vinculum dashboard loaded.")


# ──────────────────────────────────────────────
#  Vinculum report navigation
# ──────────────────────────────────────────────

def navigate_to_dispatch_report(driver):
    log.info("Navigating to Reports > Outbound > Dispatch Report...")
    try:
        reports_icon = find_element_across_iframes(
            driver,
            "//a[contains(@title, 'Reports')] | //i[contains(@class, 'fa-book')] | //a[.//span[contains(text(), 'Reports')]]",
            by=By.XPATH,
        )
        js_click(driver, reports_icon)
    except TimeoutException:
        reports_icon = find_element_across_iframes(
            driver, "div.sidebar-menu a:nth-child(6), a[href*='Report']", timeout=5
        )
        js_click(driver, reports_icon)
    time.sleep(3)

    dispatch_link = find_element_across_iframes(
        driver, "//a[contains(text(), 'Dispatch Report')]", by=By.XPATH
    )
    js_click(driver, dispatch_link)


def apply_filter_and_print(driver, start_date, end_date):
    date_fmt = "%d/%m/%Y"
    start_str = start_date.strftime(date_fmt)
    end_str = end_date.strftime(date_fmt)
    log.info("Applying date filter: %s to %s", start_str, end_str)

    date_field = find_element_across_iframes(
        driver, ".form-control[name*='Date'], input[id*='Date']", timeout=10
    )

    # Try jQuery daterangepicker API — check current window AND parent window
    # because the picker may be initialised in the parent frame when inside an iframe
    set_via_js = driver.execute_script("""
        var el = arguments[0], start = arguments[1], end = arguments[2];
        var contexts = [window, window.parent, window.top];
        for (var i = 0; i < contexts.length; i++) {
            try {
                var $ = contexts[i].jQuery || contexts[i].$;
                if ($ && $(el).data('daterangepicker')) {
                    var picker = $(el).data('daterangepicker');
                    picker.setStartDate(start);
                    picker.setEndDate(end);
                    picker.clickApply();
                    return 'api:ctx' + i;
                }
            } catch(e) {}
        }
        return false;
    """, date_field, start_str, end_str)

    if set_via_js:
        log.info("Dates set via jQuery API (%s).", set_via_js)
        time.sleep(1)
    else:
        # Open the picker, click Custom Range, then inject dates via JS only
        # (no switch_to.default_content — that crashes Chrome; no ActionChains
        #  on invisible elements)
        log.info("jQuery API unavailable — opening picker and injecting dates via JS...")
        js_click(driver, date_field)
        time.sleep(2)

        try:
            custom = find_element_across_iframes(
                driver, "//li[contains(text(), 'Custom Range')]", by=By.XPATH, timeout=5
            )
            js_click(driver, custom)
            time.sleep(1.5)
        except TimeoutException:
            pass

        date_set = driver.execute_script("""
            var start = arguments[0], end = arguments[1];
            function setVal(el, val) {
                el.removeAttribute('readonly');
                el.style.display = 'block';
                el.style.visibility = 'visible';
                var setter = Object.getOwnPropertyDescriptor(
                    HTMLInputElement.prototype, 'value').set;
                setter.call(el, val);
                el.dispatchEvent(new Event('input',  {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                el.dispatchEvent(new KeyboardEvent('keyup', {bubbles: true}));
            }
            var si = document.querySelector(
                'input[name="daterangepicker_start"], .daterangepicker input.input-mini');
            var ei = document.querySelector('input[name="daterangepicker_end"]');
            if (!ei) {
                var minis = document.querySelectorAll('.daterangepicker input.input-mini');
                if (minis.length > 1) ei = minis[1];
            }
            if (!si) return false;
            setVal(si, start);
            if (ei) setVal(ei, end);
            return true;
        """, start_str, end_str)

        if date_set:
            log.info("Dates injected via JS value setter.")
        else:
            log.warning("Could not locate picker inputs — report will use previous filter.")
        time.sleep(0.5)

        try:
            apply_btn = find_element_across_iframes(
                driver, "//button[contains(text(), 'Apply')]", by=By.XPATH, timeout=5
            )
            js_click(driver, apply_btn)
        except TimeoutException:
            pass
        time.sleep(1)

    log.info("Clicking 'Print' to generate report...")
    print_btn = find_element_across_iframes(
        driver, "#btnPrint, button[id*='Print'], .btn-warning", timeout=10
    )
    js_click(driver, print_btn)
    time.sleep(5)


def navigate_back_to_report(driver):
    """Go back to the Dispatch Report filter tab for the next batch."""
    log.info("Navigating back to Dispatch Report filter page...")
    try:
        report_tab = find_element_across_iframes(
            driver,
            "//ul[contains(@class,'nav-tabs')]//a[not(contains(text(),'Pending')) and contains(text(),'Report')]",
            by=By.XPATH,
            timeout=5,
        )
        js_click(driver, report_tab)
        time.sleep(2)
    except TimeoutException:
        log.info("Tab not found, re-navigating via sidebar...")
        navigate_to_dispatch_report(driver)

    # Click Reset to clear previous filter if available
    try:
        reset_btn = find_element_across_iframes(
            driver,
            "//button[contains(text(),'Reset')] | //input[@value='Reset']",
            by=By.XPATH,
            timeout=5,
        )
        js_click(driver, reset_btn)
        time.sleep(1)
    except TimeoutException:
        pass


def merge_batch_files(file_list, output_path):
    """Merge multiple CSVs into one deduplicated file."""
    dfs = []
    for f in file_list:
        try:
            df = pd.read_csv(f, low_memory=False)
            log.info("Read %d rows from %s", len(df), os.path.basename(f))
            dfs.append(df)
        except Exception as e:
            log.warning("Could not read %s: %s", f, e)

    if not dfs:
        raise ValueError("No batch data to merge.")

    merged = pd.concat(dfs, ignore_index=True).drop_duplicates()
    log.info("Merged total: %d rows (after dedup)", len(merged))
    merged.to_csv(output_path, index=False)
    log.info("Merged file saved: %s", output_path)
    return output_path


def poll_and_download(driver):
    log.info("Switching to Pending Report tab and polling...")
    pending_tab = find_element_across_iframes(
        driver,
        "//li[contains(., 'Pending Report')] | //a[contains(., 'Pending Report')]",
        by=By.XPATH,
    )
    js_click(driver, pending_tab)
    time.sleep(3)

    for attempt in range(1, 21):
        log.info("Poll attempt %d/20...", attempt)
        try:
            try:
                refresh = find_element_across_iframes(
                    driver, "#refreshBtn a", by=By.CSS_SELECTOR, timeout=10
                )
            except TimeoutException:
                refresh = find_element_across_iframes(
                    driver, "//*[@title='Refresh']", by=By.XPATH, timeout=5
                )
            js_click(driver, refresh)
            log.info("Refreshing Pending Report...")
            time.sleep(15)

            row = find_element_across_iframes(
                driver,
                "//table//tr[td[contains(text(), 'DispatchReportSummary')]][1]",
                by=By.XPATH,
                timeout=10,
            )
            status_cell = row.find_element(By.XPATH, "./td[2]")
            status = driver.execute_script(
                "return arguments[0].innerText;", status_cell
            ).strip()
            log.info("Current Report Status: %s", status)

            if "SUCCESS" in status.upper():
                log.info("Report SUCCESS! Downloading...")
                dl_icon = row.find_element(
                    By.XPATH, ".//img[contains(@src, 'DownloadData')]"
                )
                js_click(driver, dl_icon)
                return
        except Exception as e:
            log.warning("Polling attempt %d failed: %s", attempt, str(e).split("\n")[0])
            time.sleep(10)

    raise TimeoutException("Dispatch Report failed to generate within timeout.")


# ──────────────────────────────────────────────
#  Zoho Analytics helpers
# ──────────────────────────────────────────────

def get_zoho_access_token():
    resp = requests.post(
        "https://accounts.zoho.in/oauth/v2/token",
        data={
            "refresh_token": os.getenv("ZOHO_REFRESH_TOKEN"),
            "client_id": os.getenv("ZOHO_CLIENT_ID"),
            "client_secret": os.getenv("ZOHO_CLIENT_SECRET"),
            "grant_type": "refresh_token",
        },
    )
    resp.raise_for_status()
    return resp.json().get("access_token")


def zoho_headers():
    token = get_zoho_access_token()
    return {
        "Authorization": f"Zoho-oauthtoken {token}",
        "ZANALYTICS-ORGID": os.getenv("ZOHO_ORG_ID"),
    }


def delete_zoho_data(workspace_id, view_id, criteria):
    try:
        headers = zoho_headers()
        url = f"https://analyticsapi.zoho.in/restapi/v2/workspaces/{workspace_id}/views/{view_id}/rows"
        config = json.dumps({"criteria": criteria})
        log.info("Deleting rows with criteria: %s", criteria)

        resp = requests.delete(url, headers=headers, data={"CONFIG": config})

        if resp.status_code == 400 and "INVALID_METHOD" in resp.text:
            log.info("DELETE rejected, trying POST override...")
            headers["X-HTTP-Method-Override"] = "DELETE"
            resp = requests.post(url, headers=headers, data={"CONFIG": config})

        if resp.status_code not in (200, 204):
            log.error("Delete request failed: %s %s", resp.status_code, resp.text)
            return False
        log.info("Delete request successful: %s", resp.text[:200])
        return True
    except Exception as e:
        log.error("Error during Zoho delete: %s", e)
        return False


GEO_COLUMNS = ["City", "State", "Country"]

# Known state name corrections for Zoho geo validation
STATE_NORMALIZATIONS = {
    "andhra prasesh": "Andhra Pradesh",
    "gujrat": "Gujarat",
    "harayana": "Haryana",
    "karantaka": "Karnataka",
    "maharastra": "Maharashtra",
    "maharshtra": "Maharashtra",
    "mahrashtra": "Maharashtra",
    "telagana": "Telangana",
    "chhatishgarh": "Chhattisgarh",
    "chattisgarh": "Chhattisgarh",
    "jammu kashmir": "Jammu & Kashmir",
    "jammu and kashmir": "Jammu & Kashmir",
    "delhi ncr": "Delhi",
    "daman and diu": "Daman and Diu",
    "india": None,
    "orissa": "Odisha",
    "uttarpradesh": "Uttar Pradesh",
    "uttrakhand": "Uttarakhand",
    "dadra and nagar haveli": None,
    "daman and diu": None,
}

def clean_geo_columns(df):
    invalid_markers = {"-", ".", " ", ""}
    for col in GEO_COLUMNS:
        if col not in df.columns:
            continue
        df[col] = df[col].apply(
            lambda v: None if (pd.isna(v) or str(v).strip() in invalid_markers) else str(v).strip()
        )
        if col == "City":
            df[col] = df[col].apply(lambda v: None if (pd.notna(v) and v is not None and len(str(v)) < 2) else v)
            territory_names = {"dadra and nagar haveli", "daman and diu", "andaman and nicobar islands"}
            df[col] = df[col].apply(
                lambda v: None if (v is not None and pd.notna(v) and str(v).lower() in territory_names) else v
            )
        if col == "State":
            def _norm_state(v):
                if pd.isna(v) or v is None:
                    return None
                return STATE_NORMALIZATIONS.get(str(v).lower(), v)
            df[col] = df[col].apply(_norm_state)
    return df


def clean_data_types(df):
    # Customer Zip must be a positive number — clear non-numeric values
    if "Customer Zip" in df.columns:
        df["Customer Zip"] = pd.to_numeric(df["Customer Zip"], errors="coerce")

    # OutBound Type is incorrectly typed as Geo in Zoho view — clear unrecognized values
    # TODO: fix OutBound Type column type to Plain Text in Zoho Analytics UI to restore STO data
    if "OutBound Type" in df.columns:
        valid_outbound = {"SO", "RTV"}
        df["OutBound Type"] = df["OutBound Type"].apply(
            lambda v: None if (pd.isna(v) or str(v).strip().upper() not in valid_outbound) else v
        )
    return df


_DATE_FORMATS = [
    "%d-%m-%Y %H:%M:%S",   # 25-03-2026 13:30:00  ← Vinculum confirmed format
    "%d-%m-%Y %H:%M",      # 25-03-2026 13:30
    "%d-%m-%Y",            # 25-03-2026
    "%d/%m/%Y %I:%M %p",   # 25/03/2026 01:30 PM
    "%d/%m/%Y %H:%M:%S",   # 25/03/2026 13:30:00
    "%d/%m/%Y %H:%M",      # 25/03/2026 13:30
    "%d/%m/%Y",            # 25/03/2026
    "%m/%d/%Y %I:%M %p",   # 03/25/2026 01:30 PM  (US format)
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y",
    "%Y-%m-%d %H:%M:%S",   # ISO
    "%Y-%m-%d",
]


def parse_dates_robust(series, hint_format=None):
    """Try multiple date formats, pick the one with the fewest NaTs, and
    null out any remaining values whose year is unreasonably far in the future."""
    sample = series.dropna().head(5).tolist()
    log.info("Date sample (raw): %s", sample)

    formats = ([hint_format] if hint_format else []) + _DATE_FORMATS
    best, best_valid, best_fmt = None, -1, hint_format or "unknown"

    total_non_null = max(series.notna().sum(), 1)
    for fmt in formats:
        try:
            parsed = pd.to_datetime(series, format=fmt, errors="coerce")
        except Exception:
            continue
        valid = int(parsed.notna().sum())
        if valid > best_valid:
            best_valid, best, best_fmt = valid, parsed, fmt
        if valid >= 0.9 * total_non_null:
            break

    log.info("Best date format: '%s'  (%d / %d values parsed)", best_fmt, best_valid, total_non_null)

    # Null out dates with clearly wrong years (data-entry errors, etc.)
    current_year = datetime.now().year
    if best is not None:
        bad_mask = best.dt.year > current_year + 1
        if bad_mask.sum():
            log.warning(
                "Nulling %d dates with year > %d: %s",
                bad_mask.sum(), current_year + 1,
                best[bad_mask].dt.year.value_counts().to_dict(),
            )
            best = best.where(~bad_mask, other=pd.NaT)

        too_old = best.dt.year < 2010
        if too_old.sum():
            log.warning("Nulling %d dates with year < 2010", too_old.sum())
            best = best.where(~too_old, other=pd.NaT)

    return best


def push_to_zoho(file_path, workspace_id, view_id, date_cols=None, date_format="%d/%m/%Y %I:%M %p", num_batches=6):
    if not os.path.exists(file_path):
        log.error("File not found: %s", file_path)
        return False

    log.info("Reading data from %s...", file_path)
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception:
        df = pd.read_excel(file_path)

    df = df.replace([float("inf"), float("-inf")], None)
    df = clean_geo_columns(df)
    df = clean_data_types(df)

    # Convert float columns that should be integers (e.g. 3.0 → 3)
    for col in df.columns:
        if df[col].dtype == "float64":
            if df[col].dropna().apply(lambda x: x == int(x) if pd.notnull(x) else True).all():
                df[col] = df[col].astype("Int64")

    # Auto-collect date columns: explicit list + any column whose name contains
    # 'date' or 'time' (case-insensitive) that we haven't already listed.
    all_date_cols = list(date_cols or [])
    for col in df.columns:
        if col not in all_date_cols and any(k in col.lower() for k in ("date", "time")):
            all_date_cols.append(col)

    for col in all_date_cols:
        if col not in df.columns:
            continue
        # Parse from Vinculum's DD/MM/YYYY HH:MM AM/PM format, then output as
        # ISO (YYYY-MM-DD HH:MM:SS). ISO is unambiguous — year first means Zoho
        # cannot confuse day/month. autoIdentify:true in import_config lets Zoho
        # detect ISO automatically regardless of the column's display format.
        parsed = parse_dates_robust(df[col], hint_format=date_format)
        df[col] = parsed.apply(
            lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else None
        )
        log.info("'%s' sample after ISO conversion: %s",
                 col, df[col].dropna().head(3).tolist())

    total = len(df)
    log.info("Total rows to push: %d", total)

    if total == 0:
        return True

    # Fixed number of batches
    batch_size = max(1, -(-total // num_batches))  # ceiling division
    total_batches = (total + batch_size - 1) // batch_size
    log.info("Batch size: %d rows per batch, %d batches", batch_size, total_batches)

    headers = zoho_headers()
    url = f"https://analyticsapi.zoho.in/restapi/v2/workspaces/{workspace_id}/views/{view_id}/data"

    import_config = json.dumps({
        "importType": "updateadd",
        "fileType": "csv",
        "autoIdentify": "true",   # let Zoho detect column types & date formats automatically
        "delimiter": "0",
        "quoted": "2",
        "matchingColumns": ["Delivery No"],
    })

    all_success = True
    for i in range(0, total, batch_size):
        batch_df = df.iloc[i : i + batch_size]
        batch_num = i // batch_size + 1
        batch_csv = batch_df.to_csv(index=False)
        batch_mb = len(batch_csv.encode("utf-8")) / (1024 * 1024)
        log.info("Pushing batch %d/%d (%d rows, %.1f MB)...", batch_num, total_batches, len(batch_df), batch_mb)


        resp = requests.post(
            url,
            headers=headers,
            files={"FILE": ("data.csv", batch_csv, "text/csv")},
            data={"CONFIG": import_config},
        )

        if resp.status_code in (200, 201):
            log.info("Batch %d pushed successfully.", batch_num)
        else:
            log.error("Batch %d failed: %s %s", batch_num, resp.status_code, resp.text)
            all_success = False
            if i == 0:
                break

    return all_success


# ──────────────────────────────────────────────
#  Main pipeline
# ──────────────────────────────────────────────

def main():
    log.info("=== Dispatch Summary Automation ===")
    driver = None
    downloaded_files = []
    try:
        driver = setup_driver(DOWNLOAD_DIR)
        vinculum_login(driver)
        navigate_to_dispatch_report(driver)

        batches = get_date_batches()

        # Download one batch at a time
        for batch_num, (start_date, end_date) in enumerate(batches, 1):
            log.info("--- Batch %d/3: %s to %s ---", batch_num, start_date, end_date)

            if batch_num > 1:
                navigate_back_to_report(driver)

            apply_filter_and_print(driver, start_date, end_date)

            start_time = time.time()
            poll_and_download(driver)
            file_path = wait_for_download(DOWNLOAD_DIR, start_time)

            if not file_path:
                log.error("Batch %d: No file downloaded. Skipping.", batch_num)
                continue

            batch_path = os.path.join(DOWNLOAD_DIR, f"batch_{batch_num}.csv")
            if os.path.exists(batch_path):
                os.remove(batch_path)
            os.rename(file_path, batch_path)
            downloaded_files.append(batch_path)
            log.info("Batch %d saved: %s", batch_num, batch_path)

        if not downloaded_files:
            log.error("FAILED: No batches downloaded.")
            return

        # Merge all batches into one file
        final_path = os.path.join(DOWNLOAD_DIR, FINAL_FILENAME)
        if os.path.exists(final_path):
            os.remove(final_path)
        merge_batch_files(downloaded_files, final_path)

        # Push merged data to Zoho
        workspace_id = os.getenv("ZOHO_WORKSPACE_ID")
        view_id = os.getenv("ZOHO_DISPATCH_VIEW_ID")

        # Delete any rows with obviously wrong future dates left over from earlier bad runs
        current_year = datetime.now().year
        bad_date_criteria = f'"Order Date" > \'{current_year + 1}-01-01\''
        log.info("Deleting bad-year rows from Zoho (criteria: %s)...", bad_date_criteria)
        delete_zoho_data(workspace_id, view_id, bad_date_criteria)

        log.info("Pushing merged Dispatch Summary data to Zoho...")
        push_success = push_to_zoho(
            final_path, workspace_id, view_id,
            date_cols=["Order Date", "Delivery Date", "Ship Date", "Invoice Date"],
            date_format="%d/%m/%Y %I:%M %p",
        )

        if push_success:
            log.info("SUCCESS: Dispatch Summary pipeline complete.")
            os.remove(final_path)
            for f in downloaded_files:
                if os.path.exists(f):
                    os.remove(f)
            log.info("Cleaned up all batch and merged CSV files.")
        else:
            log.error("FAILED: Zoho push was not successful. CSVs kept for retry.")

    except Exception:
        log.exception("Automation FAILED.")
        if driver:
            try:
                driver.save_screenshot(os.path.join(DOWNLOAD_DIR, "error_dispatch.png"))
            except Exception:
                pass
    finally:
        if driver:
            driver.quit()
    log.info("=== Done ===")


if __name__ == "__main__":
    main()
