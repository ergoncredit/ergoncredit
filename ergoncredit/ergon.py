import browser_cookie3, requests, webbrowser, pandas as pd
from sqlalchemy import create_engine

def erg(api, mode="auto", user="user", password="password"):
        
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

def MySQLConect(mysql, query):
    post = mysql["mode"] + '://' + mysql["user"] + ':' + mysql["password"] + '@' + mysql["server"] + '/' + mysql["database"]
    sqlEngine       = create_engine(post, pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    return pd.read_sql(query, dbConnection)
