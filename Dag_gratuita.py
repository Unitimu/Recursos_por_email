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

# Ideias:
# 1. Tornar tudo da udacity Fixo! Então provavelmente vou dar um split!

# !!!Nova Regra a ser implementada:
# 1. 

# Afazeres:
# 1. Quando não tiver o link, criar um link que vai usar a info adquirida para pesquisar no google
# 2. 
# 2. Fazer o git clone quando a imagem for criada
# 2.1 Para que seja necessário usar as credencias do git apenas uma vez: usar git config --global credential.helper store

# Expandir o projeto:
# 1. Descrever os pré requisitos das bolsas de estudo, cursos e competições
# 3. Enviar por email quando tiver um curso da MJV, Dio ou Semantix Academy
# 4. Verificar se a Udemy ou outros sites grandes estão em promoção
# 5. Verificar competições, como as do Hackaton Brasil

# Feitos:
# 1. Base de dados que futuramente poderá ser usada para um modelo de NLP.

lista_mails = ['email_teste1@gmail.com','email_teste2@gmail.com']

default_args = {
            "owner": "airflow",
            "start_date": datetime(2019, 1, 1),
            "email_on_failure": False,
            "email_on_retry": False,
            "email": ["email_teste1@gmail.com"],
            "retries": 3,
            "retry_delay": timedelta(seconds=20)
        }


def mongoCollection():
    client = pymongo.MongoClient("mongodb+srv://user:password@cluster-name/?retryWrites=true&w=majority")

    db = client['db-gratuito']
    collection_grat = db['Links']

    return collection_grat

def download_resources_links(texto_email=''):
    path = '/opt/airflow/dags/free_monthly_learning_resources/resources/readme.md'
    # path = '/opt/airflow/dags/Testando_Airflow/teste_1.md'
    with open(path, 'r', encoding='utf-8') as links_atualizados:
        links_atualizados = links_atualizados.readlines()
        links_atualizados = links_atualizados[14:]

        for i,x in enumerate(links_atualizados):
            if x == '':
                links_atualizados.pop(i)

        sites_e_urls = {}
        for i,x in enumerate(links_atualizados):
            if x[:3] == '###':
                link = links_atualizados[i+1]
                if link[:5] == 'https':
                    sites_e_urls[x] = link
                else:
                    sites_e_urls[x] = 'Sem Link!'


    with open('/opt/airflow/dags/links_novos.md', 'r', encoding='utf-8') as links_desatualizados:
        links_ontem = links_desatualizados.readlines()
        # Comparar 'links_hoje' com 'links_ontem':
        links_a_verificar = [link_hj for link_hj in sites_e_urls.keys() if link_hj not in links_ontem]
        if links_a_verificar == []:
            return 'sem_novidades'
        # t_lav = [l for l in t_lh if l not in ['27']]
        # for link_hj in links_atualizados:
        #     if link_hj not in links_ontem:
        #         links_a_verificar.append(link_hj)


    with open('/opt/airflow/dags/links_novos.md', 'w', encoding='utf-8') as links_a_atualizar:
        for line in links_atualizados:
            links_a_atualizar.write(line)

    with open('/opt/airflow/links_a_verificar.md', 'a', encoding='utf-8') as links_a_verificar_file:
        # texto_email += '<ul>'
        collection_urls = mongoCollection()
        json_links = []

        data_hj = datetime.today().strftime('%Y-%m-%d')

        links_a_verificar_file.write('\n'+'-'*15)
        links_a_verificar_file.write(data_hj)
        links_a_verificar_file.write('-'*15 + '\n')


        for line in links_a_verificar:
            if len(line) < 3:
            #     texto_email += '\n'
                continue
            # links_a_verificar_file.write(line + '\n')
            # links_a_verificar_file.write(sites_e_urls[line] + '\n')

            texto_email += f'{line}<br>'
            texto_email += f'{sites_e_urls[line]}<br><br>'

            item = {'info':line,
                'url':sites_e_urls[line],
                'data':data_hj}

            json_links.append(item)

        # links_a_verificar_file.write('-'*30)
        collection_urls.insert_many(json_links)

        # texto_email += '</ul>'
    return texto_email

def send_email_basic(sender='email_teste@gmail.com', receiver=lista_mails, email_subject='Novos recursos do dia'):
    port = 465  # For SSL
    smtp_server = "smtp.gmail.com"
    sender_email = sender  # Enter your address
    receiver_email = receiver  # Enter receiver address
    password = 'goodpassword' # Enter your gmail password
    email_html = download_resources_links()
    if email_html == 'sem_novidades':
        return 'sem_novidades'

    message = MIMEMultipart("multipart")
    # Turn these into plain/html MIMEText objects
    part2 = MIMEText(email_html, "html")
    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    message.attach(part2)
    message["Subject"] = email_subject
    message["From"] = sender_email
    ## iterating through the receiver list
    # for mail in lista_mails:
    # message["To"] = ', '.join(receiver_email) 
    context = ssl.create_default_context()
    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(sender_email, password)
        # for mail in receiver_email:
        message["To"] = 'mim@gmail.com'
        # message['Cc'] = ', '.join(receiver_email)
        message['Cc'] = 'mim@gmail.com'

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

