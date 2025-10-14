import sqlite3
from werkzeug.security import generate_password_hash

# Lista de usuários predefinidos
ADMIN_USERS = ['tgr', 'lfdl']
NORMAL_USERS = ['hmc', 'hes', 'jbg', 'anln', 'tcj', 'cmf', 'mss']
DB_FILE = 'users_logs.db'

def initialize_database():
    """
    Cria as tabelas de usuários e logs, e popula com os usuários iniciais (sem senha).
    """
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    # Tabela de Usuários
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT,
            role TEXT NOT NULL
        )
    ''')

    # Tabela de Logs de Acesso
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS access_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            username TEXT NOT NULL,
            ip_address TEXT,
            location TEXT
        )
    ''')

    # Inserir usuários se eles ainda não existirem
    for user in ADMIN_USERS:
        cursor.execute("SELECT * FROM users WHERE username=?", (user,))
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO users (username, role) VALUES (?, 'admin')", (user,))
            print(f"Usuário administrador '{user}' criado.")

    for user in NORMAL_USERS:
        cursor.execute("SELECT * FROM users WHERE username=?", (user,))
        if cursor.fetchone() is None:
            cursor.execute("INSERT INTO users (username, role) VALUES (?, 'user')", (user,))
            print(f"Usuário comum '{user}' criado.")

    conn.commit()
    conn.close()
    print("Banco de dados inicializado com sucesso.")

if __name__ == '__main__':
    initialize_database()