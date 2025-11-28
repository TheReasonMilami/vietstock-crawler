import os.path
import time
import pandas as pd
from datetime import datetime
from selenium.webdriver import ActionChains
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from crawl_viestock import Crawler
from concurrent.futures import ThreadPoolExecutor, as_completed



class PnLCrawler(Crawler):
    def load_page(self, ticker: str):
        try:
            self.url = f"https://finance.vietstock.vn/{ticker}/tai-chinh.htm?tab=KQKD"
            print(f"[load_page] Đang mở trang: {self.url}")
            self.driver.get(self.url)
            time.sleep(3)

            current_url = self.driver.current_url
            print(f"[load_page] Trang hiện tại: {current_url}")

            # Nếu chưa đúng trang tài chính thì retry hoặc báo lỗi
            if ticker.lower() not in current_url.lower() or "tai-chinh" not in current_url:
                print(f"[load_page] ❌ Không chuyển được đến trang mã {ticker}, vẫn ở {current_url}")
                timeStamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                self.save_html(f"{ticker}_redirect_error", ticker)
                self.driver.save_screenshot(os.path.join("data", "screenshot_errors", ticker, f"{ticker}_redirect_error_{timeStamp}.png"))
                raise Exception(f"[{ticker}] Chuyển trang thất bại: Vẫn ở {current_url}")

            print(f"[load_page] ✅ Đã vào đúng trang KQKD của {ticker}")
            self.save_html(f"{ticker}_after_load_page", ticker)

        except Exception as e:
            print(f"[load_page] ❌ Lỗi khi tải trang {ticker}: {e}")

            # Ghi lại HTML lỗi để debug
            timeStamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            self.save_html(f"{ticker}_load_page_error", ticker)
            self.driver.save_screenshot(os.path.join("data", "screenshot_errors", ticker, f"{ticker}_load_page_error_{timeStamp}.png"))
            raise e  # đẩy lỗi ra ngoài nếu cần stop luôn


    def expend_by_title(
            self,
            titles: list[str] = None
    ):
        if titles is None:
            titles = [
                'III. Các khoản phải thu ngắn hạn',
                'I. Các khoản phải thu dài hạn'
            ]
        for title_text in titles:
            try:
                # tìm span với tiêu đề đưa vào
                span = WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((
                        By.XPATH,
                        f"//span[@class='report-norm-name-has-child pointer' and normalize-space(text())='{title_text}']"
                    ))
                )

                # sau đấy tìm cái dấu + bên cạnh span đó
                plus_icon = span.find_element(
                    By.XPATH,
                    "./preceding-sibling::i[contains(@class, 'fa-plus-square-o')]"
                )

                # cuộn tới và click
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", plus_icon)
                time.sleep(5)
                ActionChains(driver=self.driver).move_to_element(plus_icon).click().perform()
                print(f"Đã nhấp vào: {title_text}")
                time.sleep(3)
            except Exception as e:
                print(f"không nhấp được: {title_text}. Lỗi: {str(e)}")

    def get_available_years(self, table_id):
        header_xpath = f"//table[@id='{table_id}']//thead/tr//th"
        headers = self.driver.find_elements(By.XPATH, header_xpath)
        years = [th.text.strip() for th in headers if th.text.strip().isdigit()]
        year_to_col_idx = {year: idx for idx, year in enumerate(years)}
        return year_to_col_idx

    def click_prev_year_button(self):
        try:
            btn = self.driver.find_element(By.CSS_SELECTOR, "div[name='btn-page-2']")
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            time.sleep(1)
            btn.click()
            print("Đã bấm nút chuyển trang để hiện năm 2015")
            time.sleep(3)  # Đợi trang load lại
        except Exception as e:
            print(f"Không tìm thấy hoặc không bấm được nút chuyển trang năm: {e}")

    def get_data_for_years(self, row_titles, target_years, table_id, year_to_col_idx):
        raw_data = {year: {} for year in target_years}
        def normalize_name(s):
            import unicodedata
            return unicodedata.normalize('NFKC', s or '').replace('\u00a0', ' ').strip().lower()
        all_rows = self.driver.find_elements(By.XPATH, f"//table[@id='{table_id}']//tr[td/div/div[contains(@class, 'report-norm-name')]]")
        for title in row_titles:
            try:
                matched_row = None
                for row in all_rows:
                    try:
                        name_div = row.find_element(By.XPATH, ".//div[contains(@class, 'report-norm-name')]")
                        name_text = name_div.text
                        name_title = name_div.get_attribute('title')
                        if (normalize_name(name_text) == normalize_name(title)) or (name_title and normalize_name(name_title) == normalize_name(title)):
                            matched_row = row
                            break
                    except Exception as e:
                        continue
                if not matched_row:
                    print(f"Không tìm thấy dòng cho '{title}'")
                    for year in target_years:
                        raw_data[year][title] = ""
                    continue
                cells = matched_row.find_elements(By.XPATH, ".//td[contains(@class, 'text-right') and @data-type='value']")
                for year in target_years:
                    if year in year_to_col_idx and year_to_col_idx[year] < len(cells):
                        value = cells[year_to_col_idx[year]].text.strip()
                        parsed_value = self.parse_number(value)
                        raw_data[year][title] = parsed_value
                    else:
                        raw_data[year][title] = None
            except Exception as e:
                print(f"Không lấy được dòng '{title}': {e}")
                for year in target_years:
                    raw_data[year][title] = ""
        return raw_data

    def crawl_single_ticker(self, ticker: str) -> dict[str, dict[str, str]]:
        try:
            print(f'\n ---Bat dau crawl du lieu KQKD cho {ticker}---')
            print(f'dang tai trang {ticker}')
            self.load_page(ticker=ticker)
            print('dang doi bang du lieu KQKD xuat hien')
            try:
                WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.ID, "tbl-data-KQKD"))
                )
            except Exception as e:
                print(f"[{ticker}] ❌ Không tìm thấy bảng KQKD: {e}")
                timeStamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                self.save_html(f"{ticker}_error_wait_table", ticker)
                self.driver.save_screenshot(os.path.join("data", "screenshot_errors", ticker, f"{ticker}_error_wait_table_{timeStamp}.png"))
                raise

            print(f'Đang chọn kỳ và đơn vị tiền')
            self.select_money_unit()
            time.sleep(5)
            self.select_period()
            time.sleep(5)

            self.save_html(f'{ticker}_KQKD_after_expand', ticker)
            time.sleep(5)

            table_id = "tbl-data-KQKD"
            row_titles = [
                '3. Doanh thu thuần về bán hàng và cung cấp dịch vụ',
                '4. Giá vốn hàng bán',
                '5. Lợi nhuận gộp về bán hàng và cung cấp dịch vụ',
                '6.Doanh thu hoạt động tài chính',
                '7. Chi phí tài chính',
                'Trong đó :Chi phí lãi vay',
                '9. Chi phí bán hàng',
                '10. Chi phí quản lý doanh nghiệp',
                '11. Lợi nhuận thuần từ hoạt động kinh doanh',
                '12. Thu nhập khác',
                '14. Lợi nhuận khác',
                '15. Tổng lợi nhuận kế toán trước thuế',
                '16. Chi phí thuế TNDN hiện hành',
                '17. Chi phí thuế TNDN hoãn lại',
                '18. Lợi nhuận sau thuế thu nhập doanh nghiệp'
            ]
            target_years = ['2015', '2016', '2017', '2018']

            # Bước 1: Lấy header hiện tại
            year_to_col_idx = self.get_available_years(table_id)
            years_now = list(year_to_col_idx.keys())

            # Bước 2: Lấy dữ liệu các năm đang hiển thị
            data_now = self.get_data_for_years(row_titles, [y for y in target_years if y in years_now], table_id, year_to_col_idx)

            # Bước 3: Nếu thiếu 2015, bấm nút và lấy tiếp
            if '2015' in target_years and '2015' not in years_now:
                self.click_prev_year_button()
                year_to_col_idx2 = self.get_available_years(table_id)
                years_after = list(year_to_col_idx2.keys())
                if '2015' in years_after:
                    data_2015 = self.get_data_for_years(row_titles, ['2015'], table_id, year_to_col_idx2)
                    data_now.update(data_2015)

            # Bước 4: Tạo DataFrame
            if not data_now:
                print(f' Lỗi: không lấy được dữ liệu KQKD cho {ticker}')
                error_dir = os.path.join('data', 'screenshot_errors', ticker)
                os.makedirs(error_dir, exist_ok=True)
                timeStamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                screenshot_path = os.path.join(error_dir, f'{ticker}_{timeStamp}_KQKD_error.png')
                self.driver.save_screenshot(screenshot_path)
                print(f'đã lưu ảnh lỗi vào: {screenshot_path}')
                return ticker, pd.DataFrame()

            df = pd.DataFrame.from_dict(data_now, orient='index') # chuyển dict của get_data về df
            df.index.name = 'Year'
            df.reset_index(inplace=True)
            return ticker, df

        except Exception as e:
            print(f"[{ticker}] ❌ Lỗi: {e}")
            return ticker, pd.DataFrame()

        # finally:
        #     self.driver.quit()

def crawl_tickers_sequential(tickers: list[str], output_csv: str = None) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    """
    Crawl data for multiple tickers sequentially and combine into one DataFrame
    
    Args:
        tickers: List of ticker symbols to crawl
        output_csv: Optional path to save the combined DataFrame as CSV
        
    Returns:
        tuple: (combined DataFrame, list of (ticker, error_message) for failed crawls)
    """
    dfs = []
    failed_crawls = []
    
    # Login once before starting the crawl
    crawler = PnLCrawler()
    try:
        print("\n=== Logging in ===")
        crawler.normal_login()
        time.sleep(5)  # Wait for login to complete
        
        for ticker in tickers:
            try:
                print(f"\n=== Starting crawl for {ticker} ===")
                ticker, df = crawler.crawl_single_ticker(ticker)
                
                if not df.empty:
                    df.insert(0, 'ticker', ticker)
                    dfs.append(df)
                    print(f"✅ Successfully crawled {ticker}")
                else:
                    failed_crawls.append((ticker, "Empty DataFrame returned"))
                    print(f"❌ Failed to crawl {ticker}: Empty DataFrame")
                    
            except Exception as e:
                error_msg = str(e)
                failed_crawls.append((ticker, error_msg))
                print(f"❌ Error crawling {ticker}: {error_msg}")
                
    except Exception as e:
        print(f"❌ Login failed: {e}")
        return pd.DataFrame(), [(ticker, "Login failed") for ticker in tickers]
        
    finally:
        try:
            if hasattr(crawler, 'driver') and crawler.driver:
                try:
                    crawler.driver.quit()
                except:
                    pass
        except:
            pass
    
    # Combine all successful crawls
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Save to CSV if path provided
        if output_csv:
            try:
                combined_df.to_csv(output_csv, index=False, encoding='utf-8-sig')
                print(f"\n✅ Successfully saved combined data to {output_csv}")
            except Exception as e:
                print(f"\n❌ Error saving CSV: {e}")
    else:
        combined_df = pd.DataFrame()
        print("\n❌ No data was successfully crawled")
    
    # Print summary
    print(f"\n=== Crawl Summary ===")
    print(f"Total tickers attempted: {len(tickers)}")
    print(f"Successfully crawled: {len(dfs)}")
    print(f"Failed crawls: {len(failed_crawls)}")
    
    if failed_crawls:
        print("\nFailed tickers and errors:")
        for ticker, error in failed_crawls:
            print(f"- {ticker}: {error}")
    
    return combined_df, failed_crawls

if __name__ == '__main__':
    
    test_tickers = ['AAA', 'AAM', 'ACL', 'CAV', 'CCI', 'CCL', 'CHP', 'CII', 'CLC', 'CLL', 'CLW', 'COM', 'CSM', 'CTD', 'D2D', 'DBD', 'DCL', 'DCM', 'DGW', 'DHA', 'DHG', 'DHM', 'DMC', 'DPM', 'DRL', 'DSN', 'DVP', 'DXG', 'DXV', 'FCM', 'FIT', 'FPT', 'GDT', 'GSP', 'GTA', 'HAH', 'HAP', 'HAS', 'HAX', 'HBC', 'HDC', 'HHS', 'HII', 'HNG', 'HOT', 'HPG', 'HTI', 'HTV', 'HVX', 'IMP', 'ITC', 'KSB', 'L10', 'LAF', 'LBM', 'LDG', 'LIX', 'MCP', 'MHC', 'NT2', 'NVL', 'OPC', 'PAN', 'PC1', 'PDN', 'PET', 'PGD', 'PHC', 'PHR', 'PJT', 'PNC', 'PNJ', 'POM', 'PPC', 'PXS', 'RAL', 'REE', 'S4A', 'SAV', 'SBA', 'SC5', 'SFC', 'SFG', 'SGN', 'SHA', 'SII', 'SJD', 'SKG', 'SMA', 'SMB', 'SMC', 'SRC', 'SRF', 'ST8', 'TCO', 'THG', 'THI', 'TIP', 'TMS', 'TNC', 'TPC', 'TRA', 'TSC', 'TYA', 'VAF', 'VCF', 'VFG', 'VHC', 'VIC', 'VIP', 'VIS', 'VNG', 'VNL', 'VNM', 'VOS', 'VPD', 'VPS', 'VSC', 'VTO', 'ACC', 'BTT', 'CDC', 'CMT', 'CTI', 'CVT', 'DAG', 'DTT', 'EVE', 'GAS', 'GIL', 'GMC', 'GMD', 'HRC', 'HT1', 'IJC', 'KDC', 'LCG', 'LCM', 'LHG', 'MCG', 'MSN', 'MWG', 'NAV', 'NBB', 'NCT', 'NKG', 'NLG', 'NNC', 'NSC', 'NTL', 'NVT', 'OGC', 'PDR', 'PGC', 'PIT', 'PTB', 'PXI', 'PXT', 'QBS', 'SBT', 'SCR', 'SHI', 'SPM', 'STG', 'SVC', 'SVI', 'SZC', 'SZL', 'TAC', 'TBC', 'TCL', 'TCM', 'TLG', 'TLH', 'TMP', 'TTB', 'VNE', 'VNS', 'VPH', 'VPK', 'VSH', 'ANV', 'APC', 'BBC', 'BMC', 'BMP', 'BTP', 'DQC', 'DTA', 'HLG', 'HMC', 'HTL', 'PAC', 'PVD', 'PVT', 'RDP', 'RIC', 'SFI', 'SHP', 'TCR', 'TCT', 'TDW', 'TIE', 'TMT', 'TNA', 'TNT', 'VMD', 'AGM', 'BFC', 'BRC', 'MSH', 'STK', 'ASP', 'BCG', 'TDC', 'TDM', 'CIG', 'CLG', 'DIG', 'DLG', 'GTN', 'HAG', 'HAI', 'HAR', 'HQC', 'KAC', 'KSH', 'LGL', 'PTC', 'PTL', 'TTF', 'BCE', 'FDC', 'HU1', 'KDH', 'QCG', 'SVT', 'VRC', 'TS4', 'PPI']
    df, failed = crawl_tickers_sequential(
        tickers=test_tickers,
        output_csv='data/latest_data/240_KQKD_4.csv'
    )