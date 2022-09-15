import browser_cookie3, requests, webbrowser, pandas as pd, numpy as np, xlsxwriter
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def api(api, mode="auto", user="user", password="password"):
        
    loginUrl = "https://backoffice.ergoncredit.com.br/signin#!/"
    
    if mode == "auto":
        try:
            load_cookie = browser_cookie3.load()
            json = requests.get(api, cookies=load_cookie)  
        except:
            webbrowser.open(loginUrl, new=2)
            return "Login to Ergoncredit's BackOffice first!"
            
    if mode == "manual":
        if user =="user" or password=="password":
            return "When using manual mode you need to type your credentials."
        else:
            session = requests.Session()
            loginResponse = session.post(loginUrl, data={"username":user, "password":password}, headers=None)
            json = session.get(api)
            if json.status_code == 401:
                return "Manual mode with invalid credentials."
    return json

def api2df(api):
    df = pd.DataFrame.from_dict(pd.json_normalize(api), orient="columns")
    return df

def MySQLConnect(mysql, query):
    post = 'mysql+pymysql://' + mysql["user"] + ':' + mysql["password"] + '@' + mysql["server"] + '/' + mysql["database"]
    sqlEngine       = create_engine(post, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    return pd.read_sql(query, dbConnection)

def MSSQLConnect(mssql, query):
    post = 'mssql+pymssql://' + mssql["user"] + ':' + mssql["password"] + '@' + mssql["server"] + '/' + mssql["database"]
    sqlEngine       = create_engine(post, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    return pd.read_sql(query, dbConnection)

def statusCadastroAPR(DF):
    if (DF['cedente.statusCadastro'] == "Aprovada"):
        return 'Aprovada'
    elif (DF['cedente.statusCadastro'] == "Rejeitada"):
        return 'Rejeitada'
    else:
        return 'Pendente'

def statusCadastroAR(DF):
    statusCadastroPendente = ["Finalizando Cadastro", "Analise em Processo", "Pendencia"]
    if (DF['cedente.statusCadastro'] == "Aprovada"):
        return 'Aprovada'
    elif (np.any(DF['cedente.statusCadastro'] in statusCadastroPendente)):
        return 'Aprovada'
    else:
        return 'Rejeitada'

def Companies(status_options = False):
    
    apiCompanies = api("https://backoffice.ergoncredit.com.br/api/omie/monitoring/companies").json()
    dfComapnies = pd.DataFrame.from_dict(pd.json_normalize(apiCompanies), orient="columns")

    dfComapnies = dfComapnies.rename(columns={"razaoSocial": "cedente.razaoSocial",
                                              "cnpj": "cedente.cnpj",
                                              "_id":"cedente._id",
                                              "statusCadastro":"cedente.statusCadastro",
                                              "limiteCredito":"cedente.limiteCredito"
                                             })

    dfComapnies["cedente.dataCriacaoConta"] = pd.to_datetime(dfComapnies["dataCriacaoConta"]).dt.tz_localize(None)
    dfComapnies["cedente.safraCadastro"] = dfComapnies["cedente.dataCriacaoConta"].dt.strftime("%Y-%m")
    
    dfComapnies = dfComapnies[["cedente._id", "cedente.cnpj", "cedente.statusCadastro", "cedente.razaoSocial", "cedente.limiteCredito", "cedente.dataCriacaoConta", "cedente.safraCadastro"]]
    
    if status_options == True:
        dfComapnies['cedente.statusCadastro.AR'] = dfComapnies.apply(statusCadastroAR, axis = 1)
        dfComapnies['cedente.statusCadastro.APR'] = dfComapnies.apply(statusCadastroAPR, axis = 1)
    
    return dfComapnies

def Customers():
    
    apiCustomers = api("https://backoffice.ergoncredit.com.br/api/omie/monitoring/customers").json()
    dfCustomers = pd.DataFrame.from_dict(pd.json_normalize(apiCustomers), orient="columns")
    
    dfCustomers = dfCustomers.rename(columns={"razaoSocial": "customer.razaoSocial",
                                              "cnpj": "customer.cnpj",
                                              "_id":"customer._id",
                                              "statusCadastro":"customer.statusCadastro"
                                             })
    
    dfCustomers["customer.dataCriacaoConta"] = pd.to_datetime(dfCustomers["dataCadastro"]).dt.tz_localize(None)
    dfCustomers["customer.safraCadastro"] = dfCustomers["customer.dataCriacaoConta"].dt.strftime("%Y-%m")
    dfCustomers = dfCustomers[["customer._id", "customer.cnpj", "customer.statusCadastro", "customer.razaoSocial", "customer.dataCriacaoConta", "customer.safraCadastro", "cedente._id",]]

    return dfCustomers

def InvoicesAndAnticipations():

    apiInvoices = api("https://backoffice.ergoncredit.com.br/api/omie/monitoring/invoices").json()
    apiAnticipations = api("https://backoffice.ergoncredit.com.br/api/omie/monitoring/anticipations").json()

    dfAnticipations = pd.DataFrame.from_dict(pd.json_normalize(apiAnticipations, record_path=['faturas'],
                                                               meta=['status',"dataSolicitacao"]), orient="columns")

    dfAnticipations["dataSolicitacao"] = pd.to_datetime(dfAnticipations["dataSolicitacao"]).dt.tz_localize(None)
    dfAnticipations = dfAnticipations.drop(["_id", "codigoExpiracao"], axis=1)

    dfAnticipations = dfAnticipations.rename(columns={"taxa": "anticipation.taxa", "status": "anticipation.status",
                                                      "fatura": "invoice._id","dataSolicitacao": "anticipation.dataSolicitacao",
                                                      "valorAntecipacao": "anticipation.valor"})

    dfInvoices = pd.DataFrame.from_dict(pd.json_normalize(apiInvoices), orient="columns")
    dfInvoices["vencimento"] = pd.to_datetime(dfInvoices["vencimento"]).dt.tz_localize(None)
    dfInvoices["dataCadastro"] = pd.to_datetime(dfInvoices["dataCadastro"]).dt.tz_localize(None)

    dfInvoices = dfInvoices.rename(columns={"_id": "invoice._id", "status": "invoice.status",
                                            "valor": "invoice.valor", "dataCadastro": "invoice.dataCadastro", 
                                            "numero": "invoice.numero", "numeroDuplicata": "invoice.duplicata",
                                            "vencimento": "invoice.vencimento"
                                           })

    dfInvoicesAndAnticipations = pd.merge(dfInvoices, dfAnticipations, how='left', on='invoice._id')

    dfInvoicesAndAnticipations = dfInvoicesAndAnticipations[["invoice._id", "invoice.status", "invoice.vencimento",
                                                             "invoice.numero", "invoice.valor", "invoice.dataCadastro",
                                                             "cedente._id", "customer._id", "cedente.cnpj",
                                                             "customer.cnpj" , "cedente.razaoSocial",
                                                             "customer.razaoSocial","cedente.statusCadastro",
                                                             "customer.statusCadastro",
                                                             "anticipation.taxa","anticipation.valor",
                                                             "anticipation.status", "anticipation.dataSolicitacao"]]
    return dfInvoicesAndAnticipations

def today3oclock(delta=0):
    x = pd.to_datetime((datetime.utcnow()).replace(hour=3, minute=0, second=0, microsecond=0)) +  timedelta(days = delta)
    return x

def dfsToExcel(df_list, sheets, file_name, spaces):
    writer = pd.ExcelWriter(file_name,engine='xlsxwriter')   
    row = 0
    for dataframe in df_list:
        dataframe.to_excel(writer,sheet_name=sheets,startrow=row , startcol=1)   
        row = row + len(dataframe.index) + spaces + 1
    writer.save()

def sendMail(mail_content, mail_subject, sender_address, sender_pass, receiver_address, attach_file_name):
    message = MIMEMultipart()
    message['From'] = sender_address
    message['To'] = ", ".join(receiver_address)
    message['Subject'] = mail_subject
    message.attach(MIMEText(mail_content, 'plain'))
    attach_file = open(attach_file_name, 'rb')
    payload = MIMEBase('application', 'octate-stream')
    payload.set_payload((attach_file).read())
    encoders.encode_base64(payload)
    payload.add_header('Content-Disposition', 'attachment', filename=attach_file_name)
    message.attach(payload)
    session = smtplib.SMTP('smtp.gmail.com', 587)
    session.starttls()
    session.login(sender_address, sender_pass)
    text = message.as_string()
    session.sendmail(sender_address, receiver_address, text)
    session.quit()
    return "Enviado."

def CNAESubclasses(cnae):
    if(1 <= cnae <= 3):
        return "AGRICULTURA, PECUÁRIA, PRODUÇÃO FLORESTAL, PESCA E AQÜICULTURA"
    if(5 <= cnae <= 9):
        return "INDÚSTRIAS EXTRATIVAS"
    if(10 <= cnae <= 33):
        return "INDÚSTRIAS DE TRANSFORMAÇÃO"
    if(35 <= cnae <= 35):
        return "ELETRICIDADE E GÁS"
    if(36 <= cnae <= 39):
        return "ÁGUA, ESGOTO, ATIVIDADES DE GESTÃO DE RESÍDUOS E DESCONTAMINAÇÃO"
    if(41 <= cnae <= 43):
        return "CONSTRUÇÃO"
    if(45 <= cnae <= 47):
        return "COMÉRCIO; REPARAÇÃO DE VEÍCULOS AUTOMOTORES E MOTOCICLETAS"
    if(49 <= cnae <= 53):
        return "TRANSPORTE, ARMAZENAGEM E CORREIO"
    if(55 <= cnae <= 56):
        return "ALOJAMENTO E ALIMENTAÇÃO"
    if(58 <= cnae <= 63):
        return "INFORMAÇÃO E COMUNICAÇÃO"
    if(64 <= cnae <= 66):
        return "ATIVIDADES FINANCEIRAS, DE SEGUROS E SERVIÇOS RELACIONADOS"
    if(68 <= cnae <= 68):
        return "ATIVIDADES IMOBILIÁRIAS"
    if(69 <= cnae <= 75):
        return "ATIVIDADES PROFISSIONAIS, CIENTÍFICAS E TÉCNICAS"
    if(77 <= cnae <= 82):
        return "ATIVIDADES ADMINISTRATIVAS E SERVIÇOS COMPLEMENTARES"
    if(84 <= cnae <= 84):
        return "ADMINISTRAÇÃO PÚBLICA, DEFESA E SEGURIDADE SOCIAL"
    if(85 <= cnae <= 85):
        return "EDUCAÇÃO"
    if(86 <= cnae <= 88):
        return "SAÚDE HUMANA E SERVIÇOS SOCIAIS"
    if(90 <= cnae <= 93):
        return "ARTES, CULTURA, ESPORTE E RECREAÇÃO"
    if(94 <= cnae <= 96):
        return "OUTRAS ATIVIDADES DE SERVIÇOS"
    if(97 <= cnae <= 97):
        return "SERVIÇOS DOMÉSTICOS"
    if(99 <= cnae <= 99):
        return "ORGANISMOS INTERNACIONAIS E OUTRAS INSTITUIÇÕES EXTRATERRITORIAIS"