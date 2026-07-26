import socket
import sys

def send(sock, line):
    sock.sendall((line + "\n").encode())
    return sock.recv(4096).decode().strip()

def main():
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 6380
    s = socket.create_connection(("127.0.0.1", port))

    print(send(s, "SET foo bar"))
    print(send(s, "GET foo"))
    print(send(s, "SET name sarthak kumar"))
    print(send(s, "GET name"))
    print(send(s, "DEL foo"))
    print(send(s, "GET foo"))
    print(send(s, "GET nonexistent"))

    s.close()

if __name__ == "__main__":
    main()
