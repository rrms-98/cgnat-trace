import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

DB_CONFIG = {
    "dbname": "cgnat",
    "user": "cgnat_user",
    "password": "q1w2e3R$",
    "host": "localhost"
}

def main():
    ip = input("Digite o IP público: ").strip()
    porta = input("Digite a porta: ").strip()
    data_str = input("Digite a data/hora (YYYY-MM-DD HH:MM:SS): ").strip()

    try:
        porta = int(porta)
        data = datetime.strptime(data_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print("Porta ou data inválida.")
        return

    query = """
    WITH bloco AS (
        SELECT *
        FROM cgnat_logs
        WHERE dst_ip_pub = %s
          AND %s BETWEEN port_block_start AND port_block_end
          AND ts_start <= %s
        ORDER BY ts_start DESC
        LIMIT 1
    )
    SELECT 
        TO_CHAR(
            MIN(CASE WHEN l.event_type = 'ADD' THEN l.ts_start END),
            'DD/MM/YYYY HH24:MI:SS'
        ) AS inicio_conexao,

        COALESCE(
            TO_CHAR(
                MAX(CASE WHEN l.event_type = 'DELETE' THEN l.ts_start END),
                'DD/MM/YYYY HH24:MI:SS'
            ),
            'SESSÃO PERMANECE ATIVA ATÉ O MOMENTO DA CONSULTA'
        ) AS fim_conexao,

        l.src_ip_priv,
        l.dst_ip_pub,
        l.port_block_start,
        l.port_block_end

    FROM cgnat_logs l
    JOIN bloco b
      ON l.dst_ip_pub = b.dst_ip_pub
     AND l.src_ip_priv = b.src_ip_priv
     AND l.port_block_start = b.port_block_start
     AND l.port_block_end = b.port_block_end

    GROUP BY l.src_ip_priv, l.dst_ip_pub, l.port_block_start, l.port_block_end;
    """

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor(cursor_factory=RealDictCursor)

        cur.execute(query, (ip, porta, data))
        resultado = cur.fetchone()

        if resultado:
            print("\n============ RESULTADO ============")
            print(f"IP Privado: {resultado['src_ip_priv']}")
            print(f"IP Público: {resultado['dst_ip_pub']}")
            print(f"Bloco de Portas: {resultado['port_block_start']} - {resultado['port_block_end']}")
            print(f"Início da Conexão: {resultado['inicio_conexao']}")
            print(f"Fim da Conexão: {resultado['fim_conexao']}")
        else:
            print("Nenhum registro encontrado para os parâmetros informados.")

        cur.close()
        conn.close()

    except Exception as e:
        print("Erro ao consultar banco:", e)


if __name__ == "__main__":
    main()
