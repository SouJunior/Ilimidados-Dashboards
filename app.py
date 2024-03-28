from linkedin.webscraping_linkedin import ScraperLinkedin
import os
from dotenv import load_dotenv

load_dotenv()
company_code = 94807383  # página de testes
scraper = ScraperLinkedin(
    email=os.getenv("LINKEDIN_USERNAME"),
    password=os.getenv("LINKEDIN_PASSWORD"),
    company_code=company_code,
)
scraper.login()
scraper.extract_data()
scraper.driver.quit()
