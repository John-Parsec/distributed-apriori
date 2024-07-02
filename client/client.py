from Apriori import Apriori
from icecream import ic
import pandas as pd
import socket
import pickle

bufferSize = 2_097_152      # Tamanho do buffer de envio de dados

def send_df(client_socket: socket.socket, chunk: pd.DataFrame):
    try:
        df = pickle.dumps(chunk)
        
        if len(df) < bufferSize:
            sent = client_socket.sendall(df + b'EOF')
            print(f"[*] Enviado {len(df)} bytes")
            return
        
        while df:
            sent = client_socket.send(df)
            df = df[sent:]
            
            if len(df) < bufferSize:
                sent = client_socket.sendall(df + b'EOF')
                break
    except Exception as e:
        print(f"[*] Erro: {e}")
    
    print(f"[*] Enviado {len(df)} bytes")

def get_response(client_socket: socket.socket):
    df = b''
    while True:
        data = client_socket.recv(bufferSize)
        df += data
        print(f"[*] Recebido {len(data)} bytes")
        
        if b'EOF' in data:
            data = data.replace(b'EOF', b'')
            break
    
    df = pickle.loads(df)
    
    return df

def main():
    host = '127.0.0.1'
    port = 5050

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((host, port))
    print(f"[*] Conectado a {host}:{port}")
    
    minSupport = float(client.recv(1024).decode())
    
    df = get_response(client)
    
    print(f"[*] Executando Apriori...")
    apriori_runner = Apriori(df, minSupport=minSupport, transform_bol=True)
    apriori_df = apriori_runner.run()
    
    send_df(client, apriori_df)
    
    while True:
        response = get_response(client)
        
        df = apriori_runner.get_apriori(response)
        
        send_df(client, df)
        
        break
    
    client.close()
    
if __name__ == '__main__':
    main()