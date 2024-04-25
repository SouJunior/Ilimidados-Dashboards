from typing import Union, List

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
from selenium.webdriver.remote.webdriver import WebElement

# para executar mouse hover (temporário)
from selenium.webdriver.common.action_chains import ActionChains

from time import sleep
import os
from dotenv import load_dotenv


class ScraperLinkedin:
    """
    Classe para realizar o download de relatórios de uma página do LinkedIn.

    Atributos:
        DOWNLOAD_PATH (str): Caminho para o diretório onde os arquivos de relatórios serão salvos.
        URL_LOGIN (str): URL da página de login do LinkedIn.
        XPATH_LOGIN (str): XPath do campo de entrada de email.
        XPATH_PASSWORD (str): XPath do campo de entrada de senha.
        XPATH_SUBMIT (str): XPath do botão para efetuar o login.
        XPATH_CHAT_MINIMIZED (str): XPath do botão para minimizar o chat.
        XPATH_CHAT_HEADER (str): XPath do header do chat.
        XPATH_BUTTON_CLOSE_MODAL_COMPETITORS (str): XPath do botão para fechar o modal de competidores.
        XPATH_BUTTON_EXPORT (str): XPath do botão para exportar os dados.
        XPATH_BUTTON_DATERANGE (str): XPath do botão para selecionar o intervalo de datas.
        XPATH_LI_DATERANGE (str): XPath dos multiplos itens de intervalo de datas.
        XPATH_INPUT_RANGE_START: (str): XPath do input inicial de datas.
        XPATH_INPUT_RANGE_END: (str): XPath do input final de datas.
        XPATH_BUTTON_EXPORT_MODAL (str): XPath do botão de exportação.
        email (str): email linkedin.
        password (str): password linkedin.
        company_code (int): Código da empresa.
        driver (webdriver): Driver do Selenium.

    Métodos:
        start_driver(): Inicializa o driver do Selenium.
        login(): Efetua o login no LinkedIn.
        extract_data(): Efetua o loop para executar a exportação dos dados.
        close_chat_if_open(): Fecha o chat se estiver aberto.
        close_competitors_modal_if_open(): Fecha o modal de competidores se estiver aberto.
    """

    DOWNLOAD_PATH = os.path.join(os.getcwd(), "linkedin", "data", "raw", "temp")
    URL_LOGIN = "https://www.linkedin.com/login"
    XPATH_LOGIN = '//*[@id="username"]'
    XPATH_PASSWORD = '//*[@id="password"]'
    XPATH_SUBMIT = '//*[@data-litms-control-urn="login-submit"]'
    XPATH_CHAT_MINIMIZED = (
        "//div[contains(@class, 'msg-overlay-list-bubble--is-minimized')]"
    )
    XPATH_CHAT_HEADER = "//header[contains(@class, 'msg-overlay-bubble-header')]"
    XPATH_BUTTON_CLOSE_MODAL_COMPETITORS = (
        "//div[@aria-labelledby='org-edit-competitors-header']//button"
    )
    XPATH_BUTTON_EXPORT = "//div[contains(@class, 'org-analytics-export-by-date__cta-container')]//button[last()]"
    XPATH_BUTTON_DATERANGE = (
        "//div[contains(@class, 'org-analytics__export-modal--content')]//button"
    )
    XPATH_INPUT_RANGE_START = "//input[@name='rangeStart']"
    XPATH_INPUT_RANGE_END = "//input[@name='rangeEnd']"
    XPATH_LI_DATERANGE = "//ul[contains(@class, 'member-analytics-addon-daterange-picker__daterange-options')]//li"
    XPATH_BUTTON_EXPORT_MODAL = (
        "//div[contains(@class, 'artdeco-modal__actionbar')]//button[last()]"
    )

    def __init__(self, email, password, company_code):
        """
        Inicializa a instância da classe ScraperLinkedin

        Args:
            email (str): email linkedin.
            password (str): password linkedin.
            company_code (str): código da página a ser extraído os relatórios.
        """
        self.email = email
        self.password = password
        self.company_code = company_code
        self.driver = self.start_driver()

    def start_driver(self) -> webdriver:
        """
        Inicia o driver do Selenium.

        Returns:
            webdriver: Driver do Selenium.
        """
        if not os.path.exists(self.DOWNLOAD_PATH):
            os.makedirs(self.DOWNLOAD_PATH)
        else:
            for file in os.listdir(self.DOWNLOAD_PATH):
                os.remove(os.path.join(self.DOWNLOAD_PATH, file))

        options = Options()

        options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": self.DOWNLOAD_PATH,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
            },
        )

        try:
            driver = webdriver.Chrome(
                service=Service(ChromeDriverManager().install()), options=options
            )
            print("Driver iniciado com webdriver-manager")

        except Exception as e:
            if "'NoneType' object has no attribute 'split'" in str(e):
                print("Chrome não instalado")
                print("Driver iniciado sem webdriver-manager")
                driver = webdriver.Chrome(options=options)
            else:
                print(str(e))

        return driver

    def get_element(
        self,
        xpath: str,
        origin_element: WebElement = None,
        multiple: bool = False,
        force_waiting: bool = False,
        timeout: int = 10,
    ) -> Union[bool, WebElement, List[WebElement]]:
        """
        Obtém um elemento da pagina.

        Args:
            xpath (str): xpath do elemento.
            origin_element (WebElement, optional): elemento de origem. Padrão None.
            force_waiting (bool, optional): aguarda o elemento estar visível. Padrão False.
            multiple (bool, optional): retorna uma lista. Padrão False.


        Returns:
            Union[bool, WebElement, List[WebElement]]: Elemento ou lista de elementos da página se encontrados ou False.
        """
        origin_element = origin_element or self.driver

        try:
            if force_waiting and not multiple:
                wait = WebDriverWait(origin_element, timeout)
                return wait.until(EC.element_to_be_clickable((By.XPATH, xpath)))

            elif multiple:
                return origin_element.find_elements(By.XPATH, xpath)

            else:
                return origin_element.find_element(By.XPATH, xpath)

        except Exception as e:
            return False

    def login(self) -> bool:
        self.driver.get(self.URL_LOGIN)
        self.get_element(xpath=self.XPATH_LOGIN, force_waiting=True).send_keys(
            self.email
        )
        self.get_element(xpath=self.XPATH_PASSWORD).send_keys(self.password)
        self.get_element(xpath=self.XPATH_SUBMIT).click()
        self.close_chat_if_open()
        return True

    def extract_data(
        self, daterange: str = "d365", custom_daterange: list = ["dd/mm/yyyy"]
    ) -> bool:
        """
        Inicia o loop para execução da extração dos dados.

        Args:
            daterange (str): intervalo de datas(d15, d30, d90, d365, custom_date, 1, 15, 30, 90, 365). Padrão d15.
            custom_daterange (list, optional): lista de datas customizadas. Padrão ['dd/mm/yyyy'].

        Returns:
            bool: Retorna True se os dados foram extraídos com sucesso e False caso contrário.
        """
        extraction_urls = [
            f"https://www.linkedin.com/company/{self.company_code}/admin/analytics/updates/",
            f"https://www.linkedin.com/company/{self.company_code}/admin/analytics/visitors/",
            f"https://www.linkedin.com/company/{self.company_code}/admin/analytics/followers/",
            f"https://www.linkedin.com/company/{self.company_code}/admin/analytics/competitors/",
        ]

        for url in extraction_urls:
            self.driver.get(url)

            WebDriverWait(self.driver, 10).until(EC.url_to_be(url))

            if "competitors" in url:
                self.close_competitors_modal_if_open()

            self.get_element(xpath=self.XPATH_BUTTON_EXPORT).click()
            self.get_element(
                xpath=self.XPATH_BUTTON_DATERANGE, force_waiting=True
            ).click()
            sleep(1)
            d15, d30, d90, d365, custom_date = self.get_element(
                xpath=self.XPATH_LI_DATERANGE, multiple=True, force_waiting=True
            )
            daterange_map = {
                "d15": d15,
                "15": d15,
                "d30": d30,
                "30": d30,
                "d90": d90,
                "90": d90,
                "d365": d365,
                "365": d365,
                "custom_date": custom_date,
            }
            daterange_element = daterange_map.get(daterange)
            daterange_element.click()

            if daterange == "custom_date":
                self.select_custom_daterange(custom_daterange)
            # fluxo correto
            # self.get_element(xpath=self.XPATH_BUTTON_EXPORT_MODAL).click()

            # fluxo de testes - Executa apenas um mouse hover
            ActionChains(self.driver).move_to_element(
                self.get_element(xpath=self.XPATH_BUTTON_EXPORT_MODAL)
            ).perform()
            # fim fluxo de testes

            print("Fazendo download da extração de", url.split("/")[-2])

        return True

    def select_custom_daterange(self, daterange: list) -> bool:
        """
        Preenche o intervalo de datas customizado.

        Args:
            daterange (list): lista de datas customizadas.

        Returns:
            bool: Retorna True se as datas customizadas foram preenchidas com sucesso e False caso contrário.
        """
        input_range_start = self.get_element(xpath=self.XPATH_INPUT_RANGE_START)
        self.driver.execute_script("arguments[0].focus();", input_range_start)
        self.driver.execute_script("arguments[0].value = '';", input_range_start)
        input_range_start.send_keys(daterange[0])

        input_range_end = self.get_element(xpath=self.XPATH_INPUT_RANGE_END)
        self.driver.execute_script("arguments[0].focus();", input_range_end)
        self.driver.execute_script("arguments[0].value = '';", input_range_end)
        input_range_end.send_keys(daterange[-1])

    def close_chat_if_open(self) -> bool:
        """
        Fecha o chat se estiver aberto.

        Returns:
            bool: Retorna True se o chat foi fechado com sucesso e False caso contrário.
        """
        if not self.get_element(xpath=self.XPATH_CHAT_MINIMIZED):
            print("Chat detectado, fechando para evitar conflitos")
            self.get_element(xpath=self.XPATH_CHAT_HEADER).click()
            return True

    def close_competitors_modal_if_open(self) -> bool:
        """
        Fecha o modal de competidores se estiver aberto.

        Returns:
            bool: Retorna True se o modal foi fechado com sucesso e False caso contrário.
        """
        sleep(1)
        try:
            self.get_element(
                xpath=self.XPATH_BUTTON_CLOSE_MODAL_COMPETITORS, force_waiting=True
            ).click()
            return True
        except Exception as e:
            pass


if __name__ == "__main__":
    load_dotenv()
    username = os.getenv("LINKEDIN_USERNAME")
    password = os.getenv("LINKEDIN_PASSWORD")
    company_code = 94807383  # página de teste
    scraper = ScraperLinkedin(username, password, company_code)
    scraper.login()
    scraper.extract_data(daterange="15")
