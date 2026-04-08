#!/usr/bin/env python3

import argparse
import os
import socket
import sys
from typing import Tuple

HOST = "127.0.0.1"
PORT = 9000


class ProtocolError(Exception):
    pass


def recv_line(sock: socket.socket) -> str:
    data = bytearray()
    while True:
        chunk = sock.recv(1)
        if not chunk:
            raise ProtocolError("Server zavrel spojenie pred ukoncenim riadku")
        if chunk == b"\n":
            return data.decode("utf-8", errors="replace")
        data.extend(chunk)


def recv_exact(sock: socket.socket, length: int) -> bytes:
    data = bytearray()
    while len(data) < length:
        chunk = sock.recv(length - len(data))
        if not chunk:
            raise ProtocolError("Server zavrel spojenie pred odoslanim vsetkych dat")
        data.extend(chunk)
    return bytes(data)


def connect() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    return sock


def cmd_list() -> int:
    with connect() as sock:
        sock.sendall(b"LIST\n")
        header = recv_line(sock)
        parts = header.split(" ", 2)
        if len(parts) != 3 or parts[0] != "200" or parts[1] != "OK":
            print(header)
            return 1

        try:
            count = int(parts[2])
        except ValueError:
            raise ProtocolError(f"Neplatny pocet poloziek v odpovedi: {header}")

        print(f"Pocet suborov: {count}")
        for _ in range(count):
            line = recv_line(sock)
            line_parts = line.split(" ", 1)
            if len(line_parts) == 2:
                print(f"{line_parts[0]} | {line_parts[1]}")
            else:
                print(line)
    return 0


def parse_get_header(header: str) -> Tuple[int, str]:
    parts = header.split(" ", 3)
    if len(parts) < 3:
        raise ProtocolError(f"Neplatna GET odpoved: {header}")

    if parts[0] == "404" and parts[1] == "NOT_FOUND":
        raise FileNotFoundError("404 NOT_FOUND")

    if len(parts) != 4 or parts[0] != "200" or parts[1] != "OK":
        raise ProtocolError(f"Neocakavana GET odpoved: {header}")

    try:
        length = int(parts[2])
    except ValueError:
        raise ProtocolError(f"Neplatna dlzka dat v GET odpovedi: {header}")

    return length, parts[3]


def cmd_get(file_hash: str, output: str | None) -> int:
    with connect() as sock:
        sock.sendall(f"GET {file_hash}\n".encode("utf-8"))
        header = recv_line(sock)

        try:
            length, description = parse_get_header(header)
        except FileNotFoundError:
            print("404 NOT_FOUND")
            return 1

        data = recv_exact(sock, length)

    if output is None:
        output = f"down_{file_hash}"
    elif not os.path.basename(output).startswith("down_"):
        output = f"down_{output}"

    with open(output, "wb") as f:
        f.write(data)

    print(f"Stiahnute: {description}")
    print(f"Ulozene do: {output}")
    print(f"Bajty: {length}")
    return 0


def parse_upload_response(line: str) -> int:
    parts = line.split(" ")
    if len(parts) == 3 and parts[0] == "200" and parts[1] == "STORED":
        print(f"200 STORED {parts[2]}")
        return 0
    if len(parts) == 3 and parts[0] == "409" and parts[1] == "HASH_EXISTS":
        print(f"409 HASH_EXISTS {parts[2]}")
        return 0

    print(line)
    return 1


def cmd_upload(file_path: str, description: str) -> int:
    with open(file_path, "rb") as f:
        data = f.read()

    with connect() as sock:
        sock.sendall(f"UPLOAD {len(data)} {description}\n".encode("utf-8"))
        sock.sendall(data)
        line = recv_line(sock)

    return parse_upload_response(line)


def cmd_upload_stdin(description: str) -> int:
    data = sys.stdin.buffer.read()
    with connect() as sock:
        sock.sendall(f"UPLOAD {len(data)} {description}\n".encode("utf-8"))
        sock.sendall(data)
        line = recv_line(sock)

    return parse_upload_response(line)


def cmd_delete(file_hash: str) -> int:
    with connect() as sock:
        sock.sendall(f"DELETE {file_hash}\n".encode("utf-8"))
        line = recv_line(sock)

    if line == "200 OK":
        print(line)
        return 0

    print(line)
    return 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="HASHSTORE klient")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="vypis zoznam suborov")

    p_get = sub.add_parser("get", help="stiahni subor podla hash")
    p_get.add_argument("hash", help="hash suboru")
    p_get.add_argument("--output", help="nazov vystupneho suboru (predvolene down_<hash>)")

    p_upload = sub.add_parser("upload", help="nahraj subor z disku")
    p_upload.add_argument("subor", help="cesta k suboru")
    p_upload.add_argument("description", nargs="+", help="popis suboru")

    p_upload_stdin = sub.add_parser("upload-stdin", help="nahraj data zo stdin")
    p_upload_stdin.add_argument("description", nargs="+", help="popis suboru")

    p_delete = sub.add_parser("delete", help="zmaz subor podla hash")
    p_delete.add_argument("hash", help="hash suboru")

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        if args.command == "list":
            return cmd_list()
        if args.command == "get":
            return cmd_get(args.hash, args.output)
        if args.command == "upload":
            if not os.path.isfile(args.subor):
                print(f"Subor neexistuje: {args.subor}")
                return 1
            return cmd_upload(args.subor, " ".join(args.description))
        if args.command == "upload-stdin":
            return cmd_upload_stdin(" ".join(args.description))
        if args.command == "delete":
            return cmd_delete(args.hash)

        parser.print_help()
        return 1

    except (ConnectionRefusedError, TimeoutError, OSError) as e:
        print(f"Chyba spojenia: {e}")
        return 1
    except ProtocolError as e:
        print(f"Chyba protokolu: {e}")
        return 1
    except Exception as e:
        print(f"Neocakavana chyba: {e}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
