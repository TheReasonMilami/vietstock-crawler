# Vietstock Crawler

Automated tool for scraping financial statement data from [finance.vietstock.vn](https://finance.vietstock.vn/) — supports **Balance Sheet (CDKT)**, **Income Statement (KQKD)**, and **Cash Flow Statement (LCTT)** for ~240 Vietnamese stock tickers, exporting results to CSV files.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Activity Diagrams](#activity-diagrams)
- [Tech Stack](#tech-stack)
- [Installation & Usage](#installation--usage)
- [Configuration](#configuration)
- [Output](#output)
- [Known Issues & TODOs](#known-issues--todos)
- [Phiên bản Tiếng Việt](#phiên-bản-tiếng-việt)

---

## Overview

| Field | Detail |
|---|---|
| **Purpose** | Collect multi-period financial statement data for Vietnamese equities |
| **Data Source** | `finance.vietstock.vn` (login required) |
| **Period** | Fiscal year, default 9 periods (from 2015 onward) |
| **Currency Unit** | ×1,000 VND |
| **Storage** | CSV on filesystem, no database |
| **Anti-bot** | `undetected_chromedriver` + headless Chrome |

---

## Project Structure

```
vietstock-crawler/
│
├── crawl_viestock.py           # Base Crawler class (shared)
├── balance_sheet_crawler.py    # Balance Sheet (CDKT) crawler
├── profit_n_lost_crawler.py    # Income Statement (KQKD) crawler
├── cash_flow_crawler.py        # Cash Flow (LCTT) crawler
│
├── .gitignore
└── README.md
```

**Runtime output (gitignored):**

```
data/
├── latest_data/                        # ← CSV result files
├── html_logs/<ticker>/                 # ← HTML snapshots for debugging
└── screenshot_errors/<ticker>/         # ← Screenshots on failure
```

**Class inheritance:**

```
Crawler  (crawl_viestock.py)
├── BlsCrawler       (balance_sheet_crawler.py)
├── PnLCrawler       (profit_n_lost_crawler.py)
└── CashFlowCrawler  (cash_flow_crawler.py)
```

---

## Activity Diagrams

### Overall Flow

```mermaid
flowchart TD
    A([Start\n__main__]) --> B[Build ticker list\nand output CSV path]
    B --> C[crawl_tickers_sequential]

    C --> D[Instantiate Crawler\nBlsCrawler / PnLCrawler / CashFlowCrawler]
    D --> E[Launch headless Chrome\nundetected_chromedriver]
    E --> F[Login to Vietstock\nnormal_login]
    F --> G{Login\nsuccessful?}

    G -- No --> Z1([Stop / Login error])
    G -- Yes --> H[Loop over each ticker]

    H --> I[crawl_single_ticker]
    I --> J[Navigate to financial page\nfinance.vietstock.vn/TICKER/tai-chinh.htm]
    J --> K[Set currency unit\nselect_money_unit]
    K --> L[Set report period\nselect_period — 9 periods / Year]
    L --> M{Statement type?}

    M -- CDKT --> N[Expand collapsed rows\nexpand collapsible rows]
    M -- KQKD / LCTT --> O[Read table directly]
    N --> P[Get available years\nget_available_years]
    O --> P

    P --> Q[Scrape data\nget_data_for_years]
    Q --> R{Missing years\n2015–2018?}

    R -- Yes --> S[Click pagination button\nbtn-page-2 → re-scrape]
    S --> T[parse_number → DataFrame]
    R -- No --> T

    T --> U{Next ticker?}
    U -- Yes --> H
    U -- No --> V[Concatenate all DataFrames\npd.concat]
    V --> W[Export CSV\ndata/latest_data/*.csv]
    W --> X[Close browser\ndriver.quit]
    X --> Y([End])
```

---

### Per-Ticker Sequence

```mermaid
sequenceDiagram
    participant M as __main__
    participant C as Crawler
    participant WD as Chrome / WebDriver
    participant VS as finance.vietstock.vn

    M->>C: crawl_single_ticker(ticker)
    C->>WD: driver.get(URL)
    WD->>VS: HTTP GET /TICKER/tai-chinh.htm?tab=...
    VS-->>WD: Financial page HTML
    WD-->>C: Page loaded

    C->>WD: select_money_unit(1000)
    C->>WD: select_period("9 Periods", "Year")
    WD->>VS: AJAX reload table
    VS-->>WD: Table updated

    C->>WD: get_available_years()
    WD-->>C: [2024, 2023, ..., 2019]

    loop Each required year
        C->>WD: Read matching row in table
        WD-->>C: Raw text
        C->>C: parse_number(raw_text)
    end

    alt Years 2015–2018 needed
        C->>WD: Click btn-page-2
        WD->>VS: Request older data page
        VS-->>WD: Historical table
        C->>WD: get_data_for_years() round 2
        WD-->>C: Data for 2015–2018
    end

    C->>C: Build pandas DataFrame
    C-->>M: DataFrame (ticker, year, line items...)
```

---

### Class Diagram

```mermaid
classDiagram
    class Crawler {
        +driver: WebDriver
        +options: ChromeOptions
        +__init__()
        +normal_login(email, password)
        +select_period(period, unit)
        +select_money_unit(unit)
        +parse_number(text) float
        +save_html(ticker, name)
        +export_csv()  ⚠️ not implemented
    }

    class BlsCrawler {
        +crawl_single_ticker(ticker, row_titles) DataFrame
        +get_available_years() list
        +get_data_for_years(years, row_titles) dict
        +expand_rows()
    }

    class PnLCrawler {
        +crawl_single_ticker(ticker, row_titles) DataFrame
        +get_available_years() list
        +get_data_for_years(years, row_titles) dict
    }

    class CashFlowCrawler {
        +crawl_single_ticker(ticker, row_titles) DataFrame
        +get_available_years() list
        +get_data_for_years(years, row_titles) dict
        +detect_method() str
    }

    Crawler <|-- BlsCrawler
    Crawler <|-- PnLCrawler
    Crawler <|-- CashFlowCrawler
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python 3.10+ |
| Browser automation | Selenium WebDriver |
| Anti-detection | `undetected-chromedriver` |
| Data processing | `pandas` |
| Browser | Google Chrome (locally installed) |
| Storage | CSV (filesystem) |

---

## Installation & Usage

### 1. Prerequisites

- Python 3.10+
- Google Chrome installed
- A Vietstock Finance account

### 2. Install dependencies

```bash
pip install undetected-chromedriver selenium pandas
```

> **Note:** No `requirements.txt` exists yet. See [issue #1](#known-issues--todos).

### 3. Set login credentials

Open the relevant crawler file, locate `crawl_tickers_sequential`, and fill in your credentials:

```python
crawler.normal_login(email='your@email.com', password='your_password')
```

> **Note:** Credentials are currently hardcoded. See [issue #2](#known-issues--todos).

### 4. Run a crawler

```bash
# Collect Balance Sheet (CDKT)
python balance_sheet_crawler.py

# Collect Income Statement (KQKD)
python profit_n_lost_crawler.py

# Collect Cash Flow Statement (LCTT)
python cash_flow_crawler.py
```

CSV results are saved to `data/latest_data/`.

---

## Configuration

All configuration is currently **hardcoded in source** (no config file):

| Parameter | Location | Default |
|---|---|---|
| Ticker list | `__main__` in each file | ~240 symbols |
| Report period | `select_period()` | 9 periods / Year |
| Currency unit | `select_money_unit()` | ×1,000 VND |
| Year range | `get_data_for_years()` | 2015 – current year |
| Output path | `output_csv` in `__main__` | `data/latest_data/` |
| Login credentials | `normal_login()` | *(empty — must fill in)* |

---

## Output

### CSV Files

| Script | Output file | Encoding |
|---|---|---|
| `balance_sheet_crawler.py` | `240_TSCD_VH_4.csv` | `utf-8-sig` |
| `profit_n_lost_crawler.py` | `240_KQKD_4.csv` | `utf-8-sig` |
| `cash_flow_crawler.py` | `240_KH_TSCD_4.csv` | *(system default)* |

**CSV column structure:**

```
ticker | Year | <line item 1> | <line item 2> | ...
```

### Debug & Error Artifacts

| Directory | Contents |
|---|---|
| `data/html_logs/<ticker>/` | HTML snapshot of the page at crawl time |
| `data/screenshot_errors/<ticker>/` | Screenshot captured on failure |

---

## Known Issues & TODOs

### Critical

- [ ] **#1 — Missing `requirements.txt`:** No dependency manifest; library versions are uncontrolled. Create `requirements.txt` or `pyproject.toml`.

- [ ] **#2 — Hardcoded credentials:** Vietstock email/password are left empty in source and must be edited manually before each run. Use environment variables (`.env` + `python-dotenv`) or CLI arguments instead.

- [ ] **#3 — Filename typo:** `crawl_viestock.py` is missing a `t` — should be `crawl_vietstock.py`. Will affect import statements if the project grows.

### Incomplete Features

- [ ] **#4 — `export_csv()` not implemented:** The method in the base `Crawler` class is only a `pass` stub. Needs a real implementation or removal.

- [ ] **#5 — `ThreadPoolExecutor` imported but unused:** `concurrent.futures` is imported in several files but all crawling runs sequentially. Implement parallel crawling for speed, or remove the dead import.

- [ ] **#6 — Google Login XPath bug:** The Google OAuth helper uses `container(@href, ...)` — invalid XPath; should be `contains(@href, ...)`. Google login currently does not work.

### Code Quality & Architecture

- [ ] **#7 — No config file:** All parameters (tickers, years, periods, output paths) are hardcoded. Extract to `config.yaml` or `.env`.

- [ ] **#8 — ~240 tickers hardcoded:** Ticker list should be read from an external CSV/JSON file for easier maintenance and updates.

- [ ] **#9 — Cash flow missing direct method support:** `CashFlowCrawler` only handles the indirect method. Direct method is skipped and unfinished.

- [ ] **#10 — Inconsistent CSV encoding:** `balance_sheet_crawler.py` and `profit_n_lost_crawler.py` use `utf-8-sig`; `cash_flow_crawler.py` uses the system default. Standardize to `utf-8-sig`.

- [ ] **#11 — No retry logic:** On network instability or Vietstock timeouts, the crawler fails immediately. Add a `@retry` decorator or backoff loop.

- [ ] **#12 — No structured logging:** All output uses `print()`. Replace with the `logging` module (INFO/WARNING/ERROR levels) with file output.

- [ ] **#13 — No unit tests:** Zero test coverage. At minimum, test `parse_number()` and mock Selenium for crawl logic.

### Long-term Improvements

- [ ] **#14 — Add CLI via `argparse`:** Allow passing tickers, period, unit, and output path from the command line instead of editing source.

- [ ] **#15 — Database export option:** Add support for writing results to SQLite or PostgreSQL in addition to CSV.

- [ ] **#16 — Dockerize:** Package Chrome + Python + dependencies into a Docker image for environment-independent execution.

- [ ] **#17 — CI/CD pipeline:** Add GitHub Actions to run crawls on a schedule (e.g., after earnings season).

---

> **Note:** This project is currently a prototype. The first commit message reads: *"ver1, just upload to save code. Need refactor in future"*.

---
---

# Phiên bản Tiếng Việt

Công cụ tự động thu thập dữ liệu báo cáo tài chính từ [finance.vietstock.vn](https://finance.vietstock.vn/) — hỗ trợ **Bảng cân đối kế toán (CDKT)**, **Kết quả kinh doanh (KQKD)** và **Lưu chuyển tiền tệ (LCTT)** cho ~240 mã chứng khoán, xuất kết quả ra file CSV.

---

## Mục lục

- [Tổng quan](#tổng-quan)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Sơ đồ hoạt động](#sơ-đồ-hoạt-động)
- [Công nghệ sử dụng](#công-nghệ-sử-dụng)
- [Cài đặt & Chạy](#cài-đặt--chạy)
- [Cấu hình](#cấu-hình)
- [Đầu ra](#đầu-ra)
- [Vấn đề / Lỗi cần giải quyết](#vấn-đề--lỗi-cần-giải-quyết)

---

## Tổng quan

| Thành phần | Mô tả |
|---|---|
| **Mục đích** | Thu thập dữ liệu báo cáo tài chính nhiều kỳ cho danh sách mã CK Việt Nam |
| **Nguồn dữ liệu** | `finance.vietstock.vn` (yêu cầu đăng nhập) |
| **Kỳ dữ liệu** | Năm tài chính, mặc định 9 kỳ (từ 2015 trở đi) |
| **Đơn vị tiền tệ** | ×1.000 VNĐ |
| **Lưu trữ** | CSV trên filesystem, không dùng database |
| **Chống bot** | `undetected_chromedriver` + Chrome headless |

---

## Cấu trúc dự án

```
vietstock-crawler/
│
├── crawl_viestock.py           # Base class Crawler (dùng chung)
├── balance_sheet_crawler.py    # Crawler bảng CĐKT
├── profit_n_lost_crawler.py    # Crawler kết quả KQKD
├── cash_flow_crawler.py        # Crawler lưu chuyển tiền tệ LCTT
│
├── .gitignore
└── README.md
```

**Kết quả sinh ra khi chạy (gitignored):**

```
data/
├── latest_data/                        # ← File CSV kết quả
├── html_logs/<ticker>/                 # ← Snapshot HTML để debug
└── screenshot_errors/<ticker>/         # ← Screenshot khi lỗi
```

**Quan hệ kế thừa:**

```
Crawler  (crawl_viestock.py)
├── BlsCrawler       (balance_sheet_crawler.py)
├── PnLCrawler       (profit_n_lost_crawler.py)
└── CashFlowCrawler  (cash_flow_crawler.py)
```

---

## Sơ đồ hoạt động

### Luồng tổng thể

```mermaid
flowchart TD
    A([Bắt đầu\n__main__]) --> B[Khởi tạo danh sách ticker\nvà đường dẫn output CSV]
    B --> C[crawl_tickers_sequential]

    C --> D[Khởi tạo Crawler\nBlsCrawler / PnLCrawler / CashFlowCrawler]
    D --> E[Khởi động Chrome headless\nundetected_chromedriver]
    E --> F[Đăng nhập Vietstock\nnormal_login]
    F --> G{Login\nthành công?}

    G -- Không --> Z1([Dừng / Lỗi đăng nhập])
    G -- Có --> H[Vòng lặp qua từng ticker]

    H --> I[crawl_single_ticker]
    I --> J[Mở URL tài chính\nfinance.vietstock.vn/TICKER/tai-chinh.htm]
    J --> K[Chọn đơn vị tiền tệ\nselect_money_unit]
    K --> L[Chọn kỳ báo cáo\nselect_period — 9 kỳ / Năm]
    L --> M{Loại báo cáo?}

    M -- CDKT --> N[Mở rộng hàng ẩn\nexpand collapsible rows]
    M -- KQKD / LCTT --> O[Đọc bảng trực tiếp]
    N --> P[Lấy danh sách năm\nget_available_years]
    O --> P

    P --> Q[Thu thập dữ liệu\nget_data_for_years]
    Q --> R{Thiếu năm\n2015–2018?}

    R -- Có --> S[Click nút phân trang\nbtn-page-2 → scrape lại]
    S --> T[parse_number → DataFrame]
    R -- Không --> T

    T --> U{Ticker tiếp theo?}
    U -- Còn --> H
    U -- Hết --> V[Ghép tất cả DataFrame\npd.concat]
    V --> W[Xuất file CSV\ndata/latest_data/*.csv]
    W --> X[Đóng trình duyệt\ndriver.quit]
    X --> Y([Kết thúc])
```

---

### Luồng xử lý một ticker

```mermaid
sequenceDiagram
    participant M as __main__
    participant C as Crawler
    participant WD as Chrome / WebDriver
    participant VS as finance.vietstock.vn

    M->>C: crawl_single_ticker(ticker)
    C->>WD: driver.get(URL)
    WD->>VS: HTTP GET /TICKER/tai-chinh.htm?tab=...
    VS-->>WD: HTML trang tài chính
    WD-->>C: Trang đã tải

    C->>WD: select_money_unit(1000)
    C->>WD: select_period("9 Kỳ", "Năm")
    WD->>VS: AJAX reload bảng dữ liệu
    VS-->>WD: Bảng đã cập nhật

    C->>WD: get_available_years()
    WD-->>C: [2024, 2023, ..., 2019]

    loop Mỗi năm cần thiết
        C->>WD: Đọc hàng tương ứng trong bảng
        WD-->>C: Raw text
        C->>C: parse_number(raw_text)
    end

    alt Cần năm 2015–2018
        C->>WD: click btn-page-2
        WD->>VS: Yêu cầu dữ liệu trang 2
        VS-->>WD: Bảng cũ hơn
        C->>WD: get_data_for_years() lần 2
        WD-->>C: Dữ liệu 2015–2018
    end

    C->>C: Tạo pandas DataFrame
    C-->>M: DataFrame (ticker, năm, chỉ tiêu...)
```

---

### Cấu trúc class

```mermaid
classDiagram
    class Crawler {
        +driver: WebDriver
        +options: ChromeOptions
        +__init__()
        +normal_login(email, password)
        +select_period(period, unit)
        +select_money_unit(unit)
        +parse_number(text) float
        +save_html(ticker, name)
        +export_csv()  ⚠️ chưa cài đặt
    }

    class BlsCrawler {
        +crawl_single_ticker(ticker, row_titles) DataFrame
        +get_available_years() list
        +get_data_for_years(years, row_titles) dict
        +expand_rows()
    }

    class PnLCrawler {
        +crawl_single_ticker(ticker, row_titles) DataFrame
        +get_available_years() list
        +get_data_for_years(years, row_titles) dict
    }

    class CashFlowCrawler {
        +crawl_single_ticker(ticker, row_titles) DataFrame
        +get_available_years() list
        +get_data_for_years(years, row_titles) dict
        +detect_method() str
    }

    Crawler <|-- BlsCrawler
    Crawler <|-- PnLCrawler
    Crawler <|-- CashFlowCrawler
```

---

## Công nghệ sử dụng

| Thành phần | Công nghệ |
|---|---|
| Ngôn ngữ | Python 3.10+ |
| Tự động hóa trình duyệt | Selenium WebDriver |
| Chống phát hiện bot | `undetected-chromedriver` |
| Xử lý dữ liệu | `pandas` |
| Trình duyệt | Google Chrome (cài đặt cục bộ) |
| Lưu trữ | CSV (filesystem) |

---

## Cài đặt & Chạy

### 1. Yêu cầu

- Python 3.10+
- Google Chrome đã cài đặt
- Tài khoản Vietstock Finance

### 2. Cài đặt thư viện

```bash
pip install undetected-chromedriver selenium pandas
```

> **Lưu ý:** Chưa có file `requirements.txt`. Xem [vấn đề #1](#vấn-đề--lỗi-cần-giải-quyết).

### 3. Cấu hình thông tin đăng nhập

Mở file crawler tương ứng, tìm đến hàm `crawl_tickers_sequential` và điền thông tin:

```python
crawler.normal_login(email='your@email.com', password='your_password')
```

> **Lưu ý:** Thông tin đăng nhập hiện bị hardcode. Xem [vấn đề #2](#vấn-đề--lỗi-cần-giải-quyết).

### 4. Chạy crawler

```bash
# Thu thập bảng cân đối kế toán (CDKT)
python balance_sheet_crawler.py

# Thu thập kết quả kinh doanh (KQKD)
python profit_n_lost_crawler.py

# Thu thập lưu chuyển tiền tệ (LCTT)
python cash_flow_crawler.py
```

Kết quả CSV được lưu tại `data/latest_data/`.

---

## Cấu hình

Tất cả cấu hình hiện tại đều **hardcode trong source code** (chưa có file config):

| Tham số | Vị trí | Mặc định |
|---|---|---|
| Danh sách ticker | `__main__` mỗi file | ~240 mã |
| Kỳ báo cáo | `select_period()` | 9 kỳ / Năm |
| Đơn vị tiền tệ | `select_money_unit()` | ×1.000 VNĐ |
| Năm cần lấy | `get_data_for_years()` | 2015 – năm hiện tại |
| Đường dẫn output | `output_csv` trong `__main__` | `data/latest_data/` |
| Thông tin đăng nhập | `normal_login()` | *(trống — phải điền)* |

---

## Đầu ra

### File CSV

| Script | File output | Encoding |
|---|---|---|
| `balance_sheet_crawler.py` | `240_TSCD_VH_4.csv` | `utf-8-sig` |
| `profit_n_lost_crawler.py` | `240_KQKD_4.csv` | `utf-8-sig` |
| `cash_flow_crawler.py` | `240_KH_TSCD_4.csv` | *(mặc định)* |

**Cấu trúc cột CSV:**

```
ticker | Year | <chỉ tiêu 1> | <chỉ tiêu 2> | ...
```

### Debug & Lỗi

| Thư mục | Nội dung |
|---|---|
| `data/html_logs/<ticker>/` | Snapshot HTML của trang tại thời điểm crawl |
| `data/screenshot_errors/<ticker>/` | Ảnh chụp màn hình khi xảy ra lỗi |

---

## Vấn đề / Lỗi cần giải quyết

### Nghiêm trọng

- [ ] **#1 — Thiếu `requirements.txt`:** Không có file khai báo dependency, phiên bản thư viện không được kiểm soát. Cần tạo `requirements.txt` hoặc `pyproject.toml`.

- [ ] **#2 — Credentials hardcode:** Email/password đăng nhập Vietstock hiện để trống trong source, phải chỉnh tay trước mỗi lần chạy. Cần dùng biến môi trường (`.env` + `python-dotenv`) hoặc tham số CLI.

- [ ] **#3 — Typo tên file:** `crawl_viestock.py` thiếu chữ `t` (đúng phải là `crawl_vietstock.py`). Ảnh hưởng tới import statement nếu mở rộng dự án.

### Tính năng chưa hoàn chỉnh

- [ ] **#4 — `export_csv()` chưa được cài đặt:** Method trong base class `Crawler` chỉ có `pass`, không có logic. Cần implement hoặc xóa bỏ.

- [ ] **#5 — `ThreadPoolExecutor` import nhưng không dùng:** `concurrent.futures` được import trong một số file nhưng toàn bộ crawl chạy tuần tự. Cần implement crawl song song để tăng tốc độ, hoặc xóa import thừa.

- [ ] **#6 — Google Login bị lỗi XPath:** Hàm Google OAuth dùng `container(@href, ...)` — không phải XPath hợp lệ, phải là `contains(@href, ...)`. Chức năng đăng nhập bằng Google hiện không hoạt động.

### Chất lượng code & kiến trúc

- [ ] **#7 — Không có file cấu hình:** Tất cả tham số (danh sách ticker, năm, kỳ, đường dẫn output) đều hardcode. Cần tách ra file `config.yaml` hoặc `.env`.

- [ ] **#8 — Danh sách ticker hardcode ~240 mã:** Cần đọc từ file CSV/JSON bên ngoài để dễ bảo trì và cập nhật.

- [ ] **#9 — Cash flow thiếu hỗ trợ phương pháp trực tiếp:** `CashFlowCrawler` hiện chỉ xử lý phương pháp gián tiếp (indirect). Phương pháp trực tiếp bị skip, chưa hoàn thiện.

- [ ] **#10 — Encoding không nhất quán:** `balance_sheet_crawler.py` và `profit_n_lost_crawler.py` dùng `utf-8-sig`, còn `cash_flow_crawler.py` dùng encoding mặc định. Cần chuẩn hóa về `utf-8-sig`.

- [ ] **#11 — Không có xử lý retry:** Khi mạng chập chờn hoặc Vietstock timeout, crawler không có cơ chế retry. Cần thêm decorator `@retry` hoặc vòng lặp với backoff.

- [ ] **#12 — Không có logging chuẩn:** Hiện dùng `print()` để log. Cần thay bằng module `logging` với level (INFO/WARNING/ERROR) và ghi ra file.

- [ ] **#13 — Thiếu unit test:** Không có bất kỳ test nào. Cần tối thiểu test `parse_number()` và mock Selenium để kiểm tra logic crawl.

### Cải tiến dài hạn

- [ ] **#14 — Thêm CLI với `argparse`:** Cho phép truyền ticker, kỳ, đơn vị, output path từ dòng lệnh thay vì sửa source code.

- [ ] **#15 — Hỗ trợ lưu vào database:** Thêm tùy chọn xuất sang SQLite hoặc PostgreSQL thay vì chỉ CSV.

- [ ] **#16 — Docker hóa:** Đóng gói Chrome + Python + deps vào Docker image để chạy trên mọi môi trường mà không cần cài thủ công.

- [ ] **#17 — CI/CD pipeline:** Thêm GitHub Actions để chạy crawl tự động theo lịch (sau mùa công bố BCTC).

---

> **Ghi chú:** Dự án hiện ở trạng thái prototype. Commit đầu tiên ghi chú: *"ver1, just upload to save code. Need refactor in future"*.
