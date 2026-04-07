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
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv

# --- Configuration ---
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DOWNLOAD_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "downloads_dispatch_summary")
FINAL_FILENAME = "Dispatch_summary.csv"


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
    WebDriverWait(driver, 60).until(
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


def apply_filter_and_print(driver):
    log.info("Applying 'Last 30 Days' filter on Dispatch Date...")
    date_field = find_element_across_iframes(
        driver, ".form-control[name*='Date'], input[id*='Date']", timeout=10
    )
    js_click(driver, date_field)
    time.sleep(2)

    last_30_opt = find_element_across_iframes(
        driver, "//li[contains(text(), 'Last 30 Days')]", by=By.XPATH
    )
    js_click(driver, last_30_opt)

    apply_btn = find_element_across_iframes(
        driver, "//button[contains(text(), 'Apply')]", by=By.XPATH
    )
    js_click(driver, apply_btn)
    time.sleep(1)

    log.info("Clicking 'Print' to generate report...")
    print_btn = find_element_across_iframes(
        driver, "#btnPrint, button[id*='Print'], .btn-warning", timeout=10
    )
    js_click(driver, print_btn)
    time.sleep(5)


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


def push_to_zoho(file_path, workspace_id, view_id, date_cols=None, date_format="%d/%m/%Y %I:%M %p", max_chunk_mb=19):
    if not os.path.exists(file_path):
        log.error("File not found: %s", file_path)
        return False

    log.info("Reading data from %s...", file_path)
    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception:
        df = pd.read_excel(file_path)

    df = df.replace([float("inf"), float("-inf")], None)

    # Convert float columns that should be integers (e.g. 3.0 → 3)
    for col in df.columns:
        if df[col].dtype == "float64":
            if df[col].dropna().apply(lambda x: x == int(x) if pd.notnull(x) else True).all():
                df[col] = df[col].astype("Int64")

    if date_cols:
        for col in date_cols:
            if col in df.columns:
                log.info("Reformatting date column: %s", col)
                df[col] = pd.to_datetime(df[col], format=date_format, errors="coerce")
                df[col] = df[col].apply(
                    lambda x: x.strftime("%Y-%m-%d %H:%M:%S") if pd.notnull(x) else None
                )

    total = len(df)
    log.info("Total rows to push: %d", total)

    if total == 0:
        return True

    # Calculate batch size to keep CSV chunks under max_chunk_mb
    sample_csv = df.head(100).to_csv(index=False).encode("utf-8")
    avg_row_bytes = len(sample_csv) / min(100, total)
    batch_size = max(100, int((max_chunk_mb * 1024 * 1024) / avg_row_bytes))
    total_batches = (total + batch_size - 1) // batch_size
    log.info("Batch size: %d rows (~%.1f MB each), %d batches", batch_size, (batch_size * avg_row_bytes) / (1024 * 1024), total_batches)

    headers = zoho_headers()
    url = f"https://analyticsapi.zoho.in/restapi/v2/workspaces/{workspace_id}/views/{view_id}/data"

    import_config = json.dumps({
        "importType": "append",
        "fileType": "csv",
        "autoIdentify": "true",
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
    try:
        # 1. Download report from Vinculum
        driver = setup_driver(DOWNLOAD_DIR)
        vinculum_login(driver)
        navigate_to_dispatch_report(driver)
        apply_filter_and_print(driver)

        start_time = time.time()
        poll_and_download(driver)
        file_path = wait_for_download(DOWNLOAD_DIR, start_time)

        if not file_path:
            log.error("FAILED: No file downloaded.")
            return

        # Rename to final filename
        final_path = os.path.join(DOWNLOAD_DIR, FINAL_FILENAME)
        if os.path.exists(final_path):
            os.remove(final_path)
        os.rename(file_path, final_path)
        log.info("Report saved: %s", final_path)

        # 2. Delete last 30 days from Zoho
        workspace_id = os.getenv("ZOHO_WORKSPACE_ID")
        view_id = os.getenv("ZOHO_DISPATCH_VIEW_ID")
        twenty_nine_days_ago = (datetime.now() - timedelta(days=29)).strftime("%Y-%m-%d 00:00:00")
        criteria = f"\"Ship Date\" >= \'{twenty_nine_days_ago}\'"

        log.info("Cleaning up last 30 days in Zoho...")
        delete_success = delete_zoho_data(workspace_id, view_id, criteria)

        # 3. Push new data to Zoho
        log.info("Pushing Dispatch Summary data to Zoho...")
        push_success = push_to_zoho(
            final_path, workspace_id, view_id, date_cols=["Dispatch Date"]
        )

        if delete_success and push_success:
            log.info("SUCCESS: Dispatch Summary pipeline complete.")
            os.remove(final_path)
            log.info("Cleaned up downloaded CSV: %s", final_path)
        else:
            log.error("FAILED: Zoho operations were not fully successful. CSV kept for retry.")

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
