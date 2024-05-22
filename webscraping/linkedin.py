from selenium import webdriver
from utils.credentials import get_credentials


user, password = get_credentials(r'.\config\credentials.yaml')

driver = webdriver.Chrome()
driver.get('https://www.linkedin.com/')

#TODO: desenvolvimento do websraping aqui!