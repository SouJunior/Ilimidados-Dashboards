
# Testes em uma Máquina Virtual sem Google Chrome

Nesse primeiro momento, estudei a melhor forma de iniciar a aplicação testando em ambientes sem nenhuma configuração prévia, apenas instalando o python e as bibliotecas necessárias (sem google chrome).
O seguinte script de teste foi baseado na [Referência de primeiro script da Documentação Selenium](https://www.selenium.dev/documentation/webdriver/getting_started/first_script/) com algumas modificações.

### Script de teste sem modificação

<img src='screenshots/sem_webdriver-manager.png'>

❌ Mensagens de erro de configuração

✅ Elementos encontrados e mensagem identificada!

*De alguma forma inicia uma instancia do google chrome básica e consegue executar o script “””Talvez puxando pelo chromium do edge”””*

### Script de teste utilizando webdriver-manager

<img src='screenshots/com_webdriver-manager.png'>

❌ Mensagens de erro de instalação de driver

❌ Não consegue iniciar uma instancia do navegador

*O webdriver-manager tenta identificar a versão do chrome instalada para buscar o driver correspondente porem quebra o fluxo durante a verificação por não encontrar*

### Resultado

Seguir sem o webdriver-menager parece ser uma boa opção visto apenas esse teste. Porém, na expectativa de poder encontrar possíveis erros, pode ser interessante seguir o caminho de utilizar um bloco try para primeiro tentar utilizar essa biblioteca e somente se retornar o erro, utilizar o modo padrão.