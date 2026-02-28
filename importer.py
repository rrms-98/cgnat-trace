import subprocess
import psycopg2
from psycopg2.extras import execute_values
import sys
import os

DB_CONFIG = {
    "dbname": "cgnat",
    "user": "cgnat_user",
    "password": "q1w2e3R$",
    "host": "localhost"
}

def process_folder(folder_path, router_name):
    # Verificação de segurança: a pasta existe?
    if not os.path.exists(folder_path):
        print(f"Erro: Pasta {folder_path} não encontrada.")
        return

    # Comando otimizado: O nfdump consegue ler arquivos nfcapd compactados 
    # se passarmos a flag de leitura recursiva correta (-R).
    # O "fmt" extrai exatamente os campos que o seu banco espera.
    cmd = f'nfdump -R {folder_path} -o "fmt:%ts %nevt %pr %sa %nda %nsa %pbstart %pbend"'

    print(f"Iniciando nfdump em: {folder_path}")
    
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        batch = []
        batch_size = 10000  # Aumentei para 10k para aproveitar seus 100GB e RAM

        for line in process.stdout:
            parts = line.strip().split()

            # O nfdump costuma entregar 9 partes com esse fmt (Data, Hora, Evento, Proto, Src, ->, Dst, PortStart, PortEnd)
            # Se a linha for cabeçalho ou resumo final, parts terá tamanho diferente.
            if len(parts) < 8 or "Date" in parts[0] or "Summary" in parts[0]:
                continue

            try:
                # Tratamento de índices baseado no output padrão do nfdump
                ts = parts[0] + " " + parts[1]
                event_type = parts[2]
                proto = parts[3]
                src_ip = parts[4]
                # parts[5] costuma ser a seta '->', então pegamos o 6
                dst_ip = parts[6]
                pbstart = parts[7]
                pbend = parts[8]

                record = (
                    ts,
                    event_type,
                    router_name,
                    int(proto) if proto.isdigit() else 0,
                    src_ip,
                    dst_ip,
                    int(pbstart),
                    int(pbend)
                )
                batch.append(record)
            except (IndexError, ValueError):
                continue

            if len(batch) >= batch_size:
                execute_values(cur, """
                    INSERT INTO cgnat_logs (
                        ts_start, event_type, router_id, protocol, 
                        src_ip_priv, dst_ip_pub, port_block_start, port_block_end
                    ) VALUES %s
                """, batch)
                conn.commit()
                batch = []

        # Insere o que sobrou no último lote
        if batch:
            execute_values(cur, """
                INSERT INTO cgnat_logs (
                    ts_start, event_type, router_id, protocol, 
                    src_ip_priv, dst_ip_pub, port_block_start, port_block_end
                ) VALUES %s
            """, batch)
            conn.commit()

        print(f"Importação de {folder_path} concluída com sucesso.")

    except Exception as e:
        print(f"Erro no banco de dados: {e}")
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Uso: python3 importer.py <caminho_pasta> <nome_roteador>")
        sys.exit(1)
    
    process_folder(sys.argv[1], sys.argv[2])
