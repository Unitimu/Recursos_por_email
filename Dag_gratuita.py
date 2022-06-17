from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.email_operator import EmailOperator
# from airflow.models import Variable
import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import pymongo
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import requests
import re
import psycopg2
import pandas as pd

# Urgente:
# 0.
# 1.Verificar a viabilidade da expansão pelo Linkedin
# 2.

# !!!Nova Regra a ser implementada:
# 1. 

# Afazeres:
# 0. Criar uma página para as pessoas cadastrarem seu email
# 1. Quando não tiver o link, criar um link que vai usar a info adquirida para pesquisar no google
# 2. Criar uma headline com as palavras chaves daquele dia e.g: Python, Java, Hackaton
# 2. Fazer o git clone quando a imagem for criada
# 2.1 Para que seja necessário usar as credencias do git apenas uma vez: usar git config --global credential.helper store

# Expandir o projeto:
# 1. Descrever os pré requisitos das bolsas de estudo, cursos e competições
# 2. https://letscode.com.br/processos-seletivos
# 3.1. Semantix Academy - Não possui um link específico com os cursos, sendo necessário avaliar outras formas de fazer isso: Talvez pelo linkedin ?
# 3.2. MJV LAB - mesma questão da 3.1
# 3.3. Hackaton Brasil - mesma questão da 3.1, talvez pelo instagram?
# 4. Verificar se a Udemy ou outros sites grandes estão em promoção
# 5. Verificar competições

# Feitos:
# 1. Base de dados que futuramente poderá ser usada para um modelo de NLP.

lista_mails = ['email_teste1@gmail.com','email_teste2@gmail.com']

default_args = {
            "owner": "airflow",
            "start_date": datetime(2019, 1, 1),
            "email_on_failure": False,
            "email_on_retry": False,
            "email": ["uiuiunitimu@gmail.com"],
            "retries": 3,
            "retry_delay": timedelta(seconds=20)
        }

def capturar_emails():
    """Seleciona os emails cadastrados"""
    conn = psycopg2.connect(
        host="host",
        database="database",
        user="user",
        password="password")

    query ="""SELECT email
    FROM main_data;"""

    emails_a_enviar = pd.read_sql(query,conn).values
    return emails_a_enviar

def mongoCollection():
    """Conectando no MongoDB e selecionando a Collection que armazenará os dados coletados"""

    client = pymongo.MongoClient("mongodb+srv://user:password@cluster-name/?retryWrites=true&w=majority")

    db = client['db-gratuito']
    collection_grat = db['Links']

    return collection_grat

def create_line_to_email(desc,link):
    """Formatando as novidades para que elas sejam lidas de forma clara no email"""

    linha = f'<li><p><strong>{desc}.&nbsp;</strong><a style="text-decoration:underline;color:#000" '
    if link == 'Sem Link!':
        linha += f'>{link}</a></p></li>'
    else:
        linha += f'target="_blank" href={link}>Link</a></p></li>'
    return linha

def data(fin='mongoDB', yesterday=False):
    """Coletando a data de hoje para múltiplas finalidades"""
    data = datetime.today()
    if yesterday:
        data -= timedelta(days=1)

    if fin == 'mongoDB':
        data_format = data.strftime('%Y-%m-%d')
    else:
        data_format = data.strftime('%d/%m/%Y')
    return data_format

def dic_to_mail_and_db(dic,json_para_collection,data_hj):
    """Loop dos dicionários que armazenam as novidades para que elas sejam enviadas por email e para o DB"""
    texto_temp = ''
    for k,val in dic.items():
        # if len(line) < 3:
        #     continue
        v = val.strip('\n')
        texto_temp += create_line_to_email(k,v)

        item_json = {'info':k,
                'url':v,
                'data':data_hj}

        json_para_collection.append(item_json)

    texto_temp += '</ul>'
    return texto_temp

def google_scrapping(soup,dic_exp):
    """Capturando Urls e Descrições das novidades que foram encontradas"""
    
    links = soup.html.find_all('div',attrs={'class':'egMi0 kCrYT'})

    if links == []:
        return 

    for x in links:
        link = x.find('a')
        desc = x.find('div',attrs={'class':'BNeawe vvjwJb AP7Wnd'})

        link = re.search('href="/url\?q=(.+)&amp;sa=U&amp', str(link)).group(1)
        desc = re.search(">(.+)</div>", str(desc)).group(1)

        dic_exp[desc] = link
    
    return dic_exp

def expansao():
    """Usando Web Scrapping + Google Dorking para encontrar novidades"""

    sites = ['udacity.com%2Fscholarships','dio.me%2Fbootcamp']
    dic_exp = {}
    data_ontem = data(yesterday=True)

    for site in sites:
        url = f'https://www.google.com/search?q=allinurl%3A%22https%3A%2F%2F{site}%22++after%3A{data_ontem}'
        page = requests.get(url)
        soup = BeautifulSoup(page.text,'html.parser')
        google_scrapping(soup,dic_exp)

    return dic_exp 


def download_resources_links():
    """Procurando todos os links novos"""

    path = '/opt/airflow/dags/free_monthly_learning_resources/resources/readme.md'
    with open(path, 'r', encoding='utf-8') as links_atualizados:
        links_atualizados = links_atualizados.readlines()

        # As 14 primeiras linhas são sempre iguais
        links_atualizados = links_atualizados[14:]

        for i,x in enumerate(links_atualizados):
            if x == '':
                links_atualizados.pop(i)
            links_atualizados[i] = x.strip('# \n')

        sites_e_urls = {}
        for i,x in enumerate(links_atualizados):
            if x[:3] == '###':
                link = links_atualizados[i+1]
                if link[:5] == 'https':
                    sites_e_urls[x] = link
                else:
                    sites_e_urls[x] = 'Sem Link!'
    
    # Comparar 'links_hoje' com 'links_ontem':
    with open('/opt/airflow/dags/links_novos.md', 'r', encoding='utf-8') as links_desatualizados:
        links_ontem = links_desatualizados.readlines()
        links_a_verificar_base = {link_hj_k:link_hj_v for link_hj_k,link_hj_v in sites_e_urls.items() if link_hj_k not in links_ontem}
    
    # Mantendo o arquivo local atualizado:
    with open('/opt/airflow/dags/links_novos.md', 'w', encoding='utf-8') as links_a_atualizar:
        for line in links_atualizados:
            links_a_atualizar.write(line)

    # Capturando novos links por web scrapping
    dic_exp = expansao()

    # Verificando se os links já foram capturados anteriormente:
    with open('/opt/airflow/dags/links_exp.txt', 'w', encoding='utf-8') as file_links_exp_ontem:
        for k in dic_exp.keys():
            if k in file_links_exp_ontem:
                del dic_exp[k]

        for link in dic_exp.keys():
            file_links_exp_ontem.write(link)


    # Verificando se não houve novidades
    if links_a_verificar_base == {} == dic_exp:
        return 'sem_novidades'

    # Preparando os dados para serem enviados e os registrando no banco de dados
    collection_urls = mongoCollection()
    json_para_collection = []
    data_hj = data()

    if dic_exp != {}:
        texto_email = '<h3>Expansão</h3><ul>'
        texto_email += dic_to_mail_and_db(dic_exp,json_para_collection,data_hj)

    if links_a_verificar_base != {}:
        texto_email = '<h3>Base</h3><ul>'
        texto_email += dic_to_mail_and_db(links_a_verificar_base,json_para_collection,data_hj)

    collection_urls.insert_many(json_para_collection)

    return texto_email

def send_email_basic():
    """Funão que envia os emails para as pessoas cadastradas"""

    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = 'uiuiunitimu@gmail.com'  # Enter your address
    receiver_email = capturar_emails()  # Enter receiver address
    password = 'goodpassword' # Enter your gmail password
    email_html = download_resources_links()
    if email_html == 'sem_novidades':
        return 'sem_novidades'

    message = MIMEMultipart("multipart")
    part2 = MIMEText(email_html, "html")

    message.attach(part2)
    data_mail = data(fin='mail')
    message["Subject"] = f'Novos recursos do dia! ({data_mail})'
    message["From"] = 'Recursos por Email <uiuiunitimu@gmail.com>' 

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)

        # Macete para que todos que recebam o email sejam anonimizadas
        message["To"] = 'mim@gmail.com'

        server.sendmail(sender_email, receiver_email, message.as_string())

    return 'Deu tudo certo!'

# def pull_function(**context): 
#     value = context['task_instance'].xcom_pull(task_ids='download_resources_links')



with DAG(dag_id="tes", schedule_interval="5 15 * * *", default_args=default_args, catchup=False,) as dag:
    # Pq não consigo executar essa linha de jeito nenhum?????
    # texto = Variable.get('texto_email')
    pulling_git_data = BashOperator(
        # dag=dag_git
        task_id="te",
        bash_command="""cd /opt/airflow/dags/free_monthly_learning_resources/
        git pull"""
    )

    download_and_send_links_task = PythonOperator(
        # dag=dag_git
        task_id="te2",
        python_callable=send_email_basic
    )
    # value = download_resources_links_task.xcom_pull(task_ids='download_resources_links_task')
    # send_email = EmailOperator( 
    #     # provide_context=True ,
    #     task_id='send_email', 
    #     to='uiuiunitimu@gmail.com', 
    #     subject='Novos recursos GIT_CLONE', 
    #     html_content='texto_email', 
    #     # dag=dag_git
    #     )

    pulling_git_data >> download_and_send_links_task #>> send_email

