import yaml


def get_credentials(credentials_path:str, credential:str = 'LinkedIn'):
    """
    Obtém as credenciais de um arquivo YAML.

    Args:
        credentials_path (str): O caminho para o arquivo YAML contendo as credenciais.
        credential (str, opcional): O tipo de credencial a ser recuperado. Padrão é 'LinkedIn'.

    Returns:
        tuple: Uma tupla contendo o usuário e a senha correspondentes à credencial especificada.

    Raises:
        FileNotFoundError: Se o arquivo especificado não puder ser encontrado.
        KeyError: Se a chave especificada não existir no arquivo YAML.
    
    Example:
        Supondo que `exemplo.yaml` contenha:
        ```yaml
        credential:
            LinkedIn:
                user: 'seu_usuario'
                password: 'sua_senha'
        ```
        A chamada da função `get_credentials('exemplo.yaml', 'LinkedIn')` retornará ('seu_usuario', 'sua_senha').
    """
    with open(credentials_path, 'r') as a:
        credentials_data = yaml.safe_load(a)

    try:
        credentials = credentials_data['credential'][credential]
        user = credentials['user']
        password = credentials['password']
        
        return (user, password)

    except KeyError as e:
        raise KeyError(f"A chave '{credential}' não existe no arquivo YAML.") from e


if __name__ == '__main__':
    user, password = get_credentials(r'.\config\credentials.yaml')
    print(user, password)