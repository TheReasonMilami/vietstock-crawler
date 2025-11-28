import os.path
import time
import re
from datetime import datetime
from typing import Optional
import undetected_chromedriver as uc
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select



class Crawler:

    def __init__(self):
        chrome_options = uc.ChromeOptions()
        prefs = {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "profile.default_content_setting_values.notifications": 2
        }
        chrome_options.headless = True
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_argument("--disable-translate")
        chrome_options.add_argument("--disable-notifications")
        chrome_options.add_argument("--disable-infobars")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--window-size=1920,1080")
        # Thêm các options để giảm khả năng bị phát hiện
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        chrome_options.add_argument("--disable-features=IsolateOrigins,site-per-process")
        chrome_options.add_argument("--disable-site-isolation-trials")
        # Thêm user agent giống người dùng thật
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

        self.driver = uc.Chrome(options=chrome_options)
        # Tăng timeout cho page load
        self.driver.set_page_load_timeout(180)  # 3 phút
        self.driver.set_script_timeout(180)
        try:
            self.driver.get("https://finance.vietstock.vn/")
        except Exception as e:
            print(f"Error loading initial page: {e}")
            # Thử load lại nếu lỗi timeout
            time.sleep(5)
            self.driver.get("https://finance.vietstock.vn/")

    def normal_login(
            self,
            email: Optional[str] = '',
            password: Optional[str] = ''
    ) -> None:
        # try:
        #     self.find_and_interact_btn(
        #         By.XPATH,
        #         locator="//a[contains(@class, 'title-link btnlogin')]",
        #         button_name="corner_login"
        #     )
        # except Exception:
        #     self.find_and_interact_btn(
        #         By.ID,
        #         locator="btn-request-call-login",
        #         button_name="pop-up_login"
        #     )
        print('bắt đầu đăng nhập')
        WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located((By.ID, 'content-login-form-input'))
        )
        # input email and pass
        self.find_and_interact_btn(
            By.NAME,
            locator='Email',
            button_name='email field',
            value=email
        )
        self.find_and_interact_btn(
            By.NAME,
            locator='Password',
            button_name='password field',
            value=password
        )
        self.find_and_interact_btn(
            By.ID,
            locator='btnLoginAccount',
            button_name='Nút Đăng nhập'
        )
        print('đã đăng nhập thành công')

    def login_with_google(
            self,
            email: Optional[str] = None,
            password: Optional[str] = None
    ) -> None:
        try:
            self.find_and_interact_btn(
                By.XPATH,
                locator="//a[contains(@class, 'title-link btnlogin')]",
                button_name="corner_login"
            )
        except Exception:
            self.find_and_interact_btn(
                By.ID,
                locator="btn-request-call-login",
                button_name="pop-up_login"
            )

        # Click at the Gmail login
        self.find_and_interact_btn(
            By.XPATH,
            locator="//a[container(@href, 'LoginGooglePlus')]",
            button_name="gmail_login"
        )

        # @These 4 below steps run on account.google.com login site
        # Enter email
        self.find_and_interact_btn(
            By.XPATH,
            locator="//input[@type='email']",
            button_name="email_input",
            value=email
        )

        # Click Next
        self.find_and_interact_btn(
            By.XPATH,
            locator="//span[text()='Tiếp theo']",
            button_name="next_btn"
        )

        # Enter password
        self.find_and_interact_btn(
            By.XPATH,
            locator="//input[@type='password']",
            value=password
        )

        # Click Next
        self.find_and_interact_btn(
            By.XPATH,
            locator="//span[text()='Tiếp theo']",
            button_name="next_btn"
        )

        time.sleep(2)

    def find_and_interact_btn(
            self,
            query_method: By,
            locator: str = None,
            value: str = None,
            wait_time: int = 15,
            button_name: str = None
    ) -> None:
        """
        :param query_method: "By" object in selenium, ex: By.XPATH
        :param locator:
        :param value:
        :param wait_time:
        :param button_name:
        """
        if locator is not None:
            try:
                WebDriverWait(self.driver, wait_time).until(
                    EC.element_to_be_clickable((query_method, locator))
                )
                button = self.driver.find_element(query_method, locator)

                if (
                    button.is_displayed()
                    and button.is_enabled()
                ):
                    if value is None:
                        button.click()
                        print(f"{button_name} is clicked")
                    else:
                        button.send_keys(value)
                        print(f"{button_name} is imported")
                else:
                    print(
                        f"{button_name} not found, assuming it's already clicked and continuing"
                    )

            except TimeoutError as e:
                raise TimeoutError(
                    f"Element not found or not clickable: {locator}"
                ) from e

        else:
            raise ValueError("locator must be passed in")

    def select_period(
            self,
            period: str = "9 Kỳ",
            period_type: str = "Năm"
    ):
        try:
            # select type of period, ex: "Năm", "Tháng", "6 tháng", ...
            period_type_dropdown = Select(
                self.driver.find_element(
                    By.XPATH,
                    '//*[@name="NumberPeriod" or @name="PeriodType"]'
                )
            )
            period_type_dropdown.select_by_visible_text(period_type)

            # select number of period, 5 is defaut and free, more period need upgrade account to payment
            period_dropdown = Select(
                self.driver.find_element(By.NAME, 'period')
            )
            period_dropdown.select_by_visible_text(period)

            # Wait for page full load
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "tr.CDKT-row-white-color")
                )
            )

            # wait more
            time.sleep(2)
        except Exception as e:
            print(f"Error selecting period type and period: {e}")

    def select_money_unit(self, type_unit: str = '1000') -> None:
        try:
            select_element = self.driver.find_element(By.NAME, 'UnitDong')
            select = Select(select_element)
            select.select_by_value(type_unit)
            print(f'đã chọn đơn v: {type_unit}')
        except Exception as e:
            print(f'lỗi khi chọn đơn vị: {e}')

    def save_html(self, file_name: str, ticker: str='UNKNOWN'):
        ''' luu html de debug, co timestamp, to chuc theo ticker'''
        try:
            html_log = self.driver.page_source
            save_dir = os.path.join('data', 'html_logs', ticker)
            os.makedirs(save_dir, exist_ok=True)

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_path = os.path.join(save_dir, f'{file_name}_{timestamp}.html')
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(html_log)
            print(f'da lu html vao file {file_name}.html')
        except Exception as e:
            print(f'loi khi luu HTML: {e}')

    def parse_number(self, value: str) -> float:
        '''chuyen so dang str sau khi cao sang dang float. Neu loi thi tra ve None'''
        try:
            clean = re.sub(r'[^\d\.\-]', '', value.replace(',', ''))
            return float(clean)
        except:
            return None

    def export_csv(self):
        pass
