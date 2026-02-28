from fastapi import FastAPI, HTTPException, Depends, Query, Body, status
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
import psycopg2
from psycopg2.extras import RealDictCursor
import os

app = FastAPI(title="Sim Digital - CGNAT Trace v2.6.0")

# Caminhos de arquivos
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
WEB_DIR = os.path.join(BASE_DIR, "web")

# Segurança e CORS
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

# Configuração do Banco de Dados
DB_CONFIG = {
    "dbname": "cgnat",
    "user": "cgnat_user",
    "password": "q1w2e3R$",
    "host": "localhost"
}

async def get_current_user(token: str = Depends(oauth2_scheme)):
    return token

@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT username, password FROM usuarios_sistema WHERE username = %s AND ativo = TRUE", (form_data.username,))
        user = cur.fetchone()
        if not user or form_data.password != user['password']:
            raise HTTPException(status_code=401, detail="Credenciais inválidas")
        return {"access_token": user['username'], "token_type": "bearer"}
    finally:
        cur.close(); conn.close()

@app.get("/")
async def read_login(): return FileResponse(os.path.join(WEB_DIR, "login.html"))

@app.get("/dashboard")
async def read_dashboard(): return FileResponse(os.path.join(WEB_DIR, "index.html"))

@app.get("/health")
async def health_check():
    try:
        conn = psycopg2.connect(**DB_CONFIG); conn.close()
        return {"status": "online"}
    except: return {"status": "offline"}

@app.get("/search")
async def search_cgnat(ip: str, porta: int, data_hora: str, fuso: str = "-03", token: str = Depends(get_current_user)):
    conn = psycopg2.connect(**DB_CONFIG); cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        query = "SELECT * FROM buscar_audit_cgnat(%s, %s, %s, %s);"
        cur.execute(query, (ip, porta, data_hora, fuso))
        res = cur.fetchone()
        if not res or res['cliente_ip'] is None: raise HTTPException(status_code=404)
        return {
            "src_ip_priv": res['cliente_ip'],
            "inicio_conexao": res['abertura_sessao'].strftime("%d/%m/%Y %H:%M:%S") if res['abertura_sessao'] else "--",
            "fim_conexao": res['fechamento_sessao'].strftime("%d/%m/%Y %H:%M:%S") if res['fechamento_sessao'] else "SESSÃO ATIVA",
            "status": res['status_no_incidente']
        }
    finally: cur.close(); conn.close()

@app.get("/usuarios/listar")
async def listar_usuarios(current_user: str = Depends(get_current_user)):
    conn = psycopg2.connect(**DB_CONFIG); cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("SELECT id, username, nome_completo FROM usuarios_sistema WHERE ativo = TRUE ORDER BY nome_completo")
        return cur.fetchall()
    finally: cur.close(); conn.close()

@app.post("/usuarios/criar")
async def criar_usuario(data: dict = Body(...), current_user: str = Depends(get_current_user)):
    conn = psycopg2.connect(**DB_CONFIG); cur = conn.cursor()
    try:
        cur.execute("INSERT INTO usuarios_sistema (username, password, nome_completo) VALUES (%s, %s, %s)", 
                    (data['username'], data['password'], data['nome']))
        conn.commit(); return {"detail": "Sucesso"}
    finally: cur.close(); conn.close()

@app.delete("/usuarios/deletar/{user_id}")
async def deletar_usuario(user_id: int, current_user: str = Depends(get_current_user)):
    if current_user != "admin": raise HTTPException(status_code=403)
    conn = psycopg2.connect(**DB_CONFIG); cur = conn.cursor()
    try:
        cur.execute("DELETE FROM usuarios_sistema WHERE id = %s", (user_id,))
        conn.commit(); return {"detail": "Removido"}
    finally: cur.close(); conn.close()
