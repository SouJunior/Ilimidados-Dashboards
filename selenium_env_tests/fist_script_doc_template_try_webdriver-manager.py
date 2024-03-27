# selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# webdriver-manager
from selenium.webdriver.chrome.service import Service

# selenium tools
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager

options = Options()

try:
    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()), options=options
    )

except AttributeError as e:
    if "'NoneType' object has no attribute 'split'" in str(e):
        print("Chrome não instalado")

    else:
        print(str(e))


if driver is None:
    driver = webdriver.Chrome(options=options)


## Test Routine

driver.get("https://www.selenium.dev/selenium/web/web-form.html")

title = driver.title

driver.implicitly_wait(0.5)

text_box = driver.find_element(by=By.NAME, value="my-text")
submit_button = driver.find_element(by=By.CSS_SELECTOR, value="button")

text_box.send_keys("Selenium")
submit_button.click()

message = driver.find_element(by=By.ID, value="message")
text = message.text
print(text)