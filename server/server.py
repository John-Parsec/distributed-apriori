from data import *
import threading
import socket
import pickle

dfs_first_fase = []             # Lista de dataframes retornados pelos clients na primeira fase
dfs_second_fase = []            # Lista de dicts com dataframes retornados pelos clients na segunda fase e a quantidade de linhas do dataframe passado originalmente
bufferSize = 2_097_152          # Tamanho do buffer de envio de dados

def split_df(df, *, chunk_size=None, parts=None):
    if chunk_size is None and parts is None:
        raise ValueError("chunk_size or parts must be provided.")
    
    if chunk_size is not None and parts is not None:
        raise ValueError("chunk_size and parts cannot be provided at the same time.")
    
    if parts is not None:
        chunk_size = len(df) // parts
    
    chunks = []
    
    for i in range(0, len(df), chunk_size):
        chunk_df = df[i:i + chunk_size]
        chunk = {
            'df': chunk_df,
            'length': len(chunk_df)
        }
        chunks.append(chunk)
        
    return chunks

def get_global_candidates(dfs):
    df = pd.concat(dfs)
    df.drop_duplicates(subset='itemsets',inplace=True)
    df = df.sort_values(by='length')
    
    return df['itemsets']

def union(dfs, len_df, len_chuncks, min_support):
    i = 0
    
    for df in dfs:
        df['support'] = round(df['support'] * (len_chuncks[i] / len_df ), 4)
        i += 1
    
    df = pd.concat(dfs)
    
    df['itemsets'] = df['itemsets'].apply(tuple)
    df = df.groupby('itemsets').agg({'support': 'sum', 'length': 'first'}).reset_index()
    df = df.sort_values(by='length')
    df = df.drop(columns=['length'])
    df = df[df['support'] >= min_support]
    
    return df

def send_df(client_socket: socket.socket, chunk: pd.DataFrame):
    try:
        df = pickle.dumps(chunk)
        print(f"[*] Enviando {len(df)} bytes")
        
        if len(df) < bufferSize:
            sent = client_socket.sendall(df + b'EOF')
            return
        
        while df:
            sent = client_socket.send(df)
            print(f"[*] Enviado {sent} bytes")
            df = df[sent:]
            
            if len(df) < bufferSize:
                sent = client_socket.sendall(df + b'EOF')
                break
    except Exception as e:
        print(f"[*] Erro: {e}")
    
def get_response(client_socket: socket.socket):
    df = b''
    while True:
        print(f"[*] Recebendo dados...")
        data = client_socket.recv(bufferSize)
        df += data
        
        if b'EOF' in data:
            data = data.replace(b'EOF', b'')
            break
    
    print(f"[*] Recebido {len(df)} bytes")
    df = pickle.loads(df)
    
    return df

def client_thread_first_fase(client_socket: socket.socket, df: pd.DataFrame):
    global dfs_first_fase
    
    send_df(client_socket, df)
    
    response = get_response(client_socket)
    
    dfs_first_fase.append(response)

def client_thread_second_fase(client_socket: socket.socket, df: pd.DataFrame, length: int):
    global dfs_second_fase
    
    send_df(client_socket, df)
    
    response = get_response(client_socket)
    
    dfs_second_fase.append({
        'df': response,
        'length': length
    })

def main():
    global dfs_first_fase, dfs_second_fase
    
    host = '127.0.0.1'
    port = 5050

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    print(f"[*] Servidor escutando em {host}:{port}")
    
    df = get_df_online_retail()
    # df = get_df_example()
    # df = get_df_exemple_2()
    
    min_support = 0.02
    
    num_workers = 4
    workers = num_workers
    clients = []
    chuncks = split_df(df, parts=workers)

    while workers > 0:        
        try:
            client, address = server.accept()
            print(f"[*] Conexão aceita de {address[0]}:{address[1]}")
            clients.append({
                'client': client,
                'length': chuncks[workers - 1]['length']
            })
            
            client.send(str(min_support).encode())

            client_handler = threading.Thread(target=client_thread_first_fase, args=(client, chuncks[workers - 1]['df']))
            client_handler.start()
            
            workers -= 1
            
        except Exception as e:
            print(f"[*] Erro: {e}")
            break

    while True:
        if len(dfs_first_fase) == num_workers:
            result = get_global_candidates(dfs_first_fase)
            
            for client in clients:
                client_handler = threading.Thread(target=client_thread_second_fase, args=(client['client'], result, client['length']))
                client_handler.start()
            
            break
    
    while True:
        if len(dfs_second_fase) == num_workers:
            len_chuncks = [client['length'] for client in clients]
            dfs = [df['df'] for df in dfs_second_fase]
            result = union(dfs, len(df), len_chuncks, min_support)
            break
    
    result.to_csv(f"results/result.csv", index=False)
    server.close()

if __name__ == '__main__':
    main()