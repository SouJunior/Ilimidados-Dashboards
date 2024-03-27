# selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# webdriver-manager
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

# selenium tools
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import time
import os
from dotenv import load_dotenv

load_dotenv()

username = os.getenv("LINKEDIN_USERNAME")
password = os.getenv("LINKEDIN_PASSWORD")
# company_code = os.getenv('LINKEDIN_COMPANY_CODE')
company_code = 94807383  # página de teste

options = Options()

try:
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )
    print("Iniciando com webdriver-manager")

except Exception as e:
    if "'NoneType' object has no attribute 'split'" in str(e):
        print("Chrome não instalado")
        print("Iniciando sem webdriver-manager")
    else:
        print(str(e))


if driver is None:
    driver = webdriver.Chrome(options=options)


driver.get("https://www.linkedin.com/login")

xpath_login = '//*[@id="username"]'
xpath_password = '//*[@id="password"]'
xpath_submit = '//*[@data-litms-control-urn="login-submit"]'


WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.XPATH, xpath_login)))

driver.find_element(by=By.XPATH, value=xpath_login).send_keys(username)
driver.find_element(by=By.XPATH, value=xpath_password).send_keys(password)
driver.find_element(by=By.XPATH, value=xpath_submit).click()

# minimize chat if open
xpath_btn_close_chat = "//*[@data-test-icon='chevron-down-small']"
                        # "//section[contains(@class, 'org-analytics-export-by-date__container')]"
try:
    driver.find_element(by=By.XPATH, value=xpath_btn_close_chat).click()
    print('Detectado chat aberto, fechando para evitar erros!')

except Exception as e:
    if "Message: no such element: Unable to locate element" in str(e):
        pass
    else:
        print(str(e))

urls = [
    f"https://www.linkedin.com/company/{company_code}/admin/analytics/updates/",
    f"https://www.linkedin.com/company/{company_code}/admin/analytics/visitors/",
    f"https://www.linkedin.com/company/{company_code}/admin/analytics/followers/",
    f"https://www.linkedin.com/company/{company_code}/admin/analytics/competitors/",
]

for url in urls:
    print("Acessando url de", url.split("/")[-2])

    driver.get(url)

    xpath_section_export = (
        "//section[contains(@class, 'org-analytics-export-by-date__container')]"
    )

    section_export = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, xpath_section_export))
    )
    
    try:
        WebDriverWait(driver, 5).until(
        EC.element_to_be_clickable((By.CLASS_NAME,"artdeco-modal__dismiss"))).click()
    
    except Exception as e:
        modal = False

    option_buttons = section_export.find_elements(by=By.TAG_NAME, value="button")

    if len(option_buttons) == 2:
        button_dates, button_export = option_buttons
    else:
        button_dates, edit_competitors, button_export = option_buttons

    button_dates.click()

    xpath_ul_date_options = "//ul[contains(@class, 'member-analytics-addon-daterange-picker__daterange-options')]"
    ul_date_options = WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, xpath_ul_date_options))
    )

    d15, d30, d90, d365, custom_date = ul_date_options.find_elements(
        by=By.TAG_NAME, value="li"
    )

    d15.click()
    print("Confirmando data de 15 dias")

    button_export.click()

    button_export_modal = driver.find_element(
        by=By.CLASS_NAME, value="artdeco-modal__actionbar"
    ).find_elements(by=By.TAG_NAME, value="button")[-1]
    button_export_modal.click()

    print("Realizando download de", url.split("/")[-2])
    time.sleep(2)
