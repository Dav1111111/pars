"""
Минимальный HTTP CONNECT прокси-форвардер.
Принимает HTTP/CONNECT запросы на локальном порту
и маршрутизирует через SOCKS5 прокси с авторизацией.
"""
import asyncio
import sys
from python_socks.async_.asyncio import Proxy


async def pipe(reader, writer):
    try:
        while not reader.at_eof():
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except (ConnectionError, asyncio.CancelledError, OSError):
        pass
    finally:
        try:
            writer.close()
        except:
            pass


async def handle_client(local_r, local_w, proxy_url):
    """Обработка одного клиентского подключения."""
    remote_w = None
    try:
        # Читаем первую строку запроса
        first_line = await asyncio.wait_for(local_r.readline(), timeout=10)
        if not first_line:
            return
        # Дочитываем заголовки
        headers = first_line
        while True:
            line = await asyncio.wait_for(local_r.readline(), timeout=10)
            headers += line
            if line == b'\r\n' or not line:
                break

        request_line = first_line.decode('utf-8', errors='ignore').strip()
        parts = request_line.split()
        if len(parts) < 2:
            return

        method = parts[0]

        if method == 'CONNECT':
            # HTTPS тоннель: CONNECT host:port HTTP/1.1
            host_port = parts[1]
            if ':' in host_port:
                host, port = host_port.rsplit(':', 1)
                port = int(port)
            else:
                host, port = host_port, 443

            proxy = Proxy.from_url(proxy_url)
            sock = await asyncio.wait_for(
                proxy.connect(dest_host=host, dest_port=port),
                timeout=15
            )
            remote_r, remote_w = await asyncio.open_connection(sock=sock)

            # Тоннель установлен
            local_w.write(b'HTTP/1.1 200 Connection Established\r\n\r\n')
            await local_w.drain()

        else:
            # Обычный HTTP: GET http://host/path HTTP/1.1
            url = parts[1]
            if url.startswith('http://'):
                url_body = url[7:]
            else:
                url_body = url
            slash_idx = url_body.find('/')
            if slash_idx == -1:
                host_port_str = url_body
                path = '/'
            else:
                host_port_str = url_body[:slash_idx]
                path = url_body[slash_idx:]
            if ':' in host_port_str:
                host, port = host_port_str.rsplit(':', 1)
                port = int(port)
            else:
                host, port = host_port_str, 80

            proxy = Proxy.from_url(proxy_url)
            sock = await asyncio.wait_for(
                proxy.connect(dest_host=host, dest_port=port),
                timeout=15
            )
            remote_r, remote_w = await asyncio.open_connection(sock=sock)

            # Переписываем запрос с абсолютного URL на относительный
            new_first = f'{method} {path} HTTP/1.1\r\n'.encode()
            rest_headers = headers[len(first_line):]
            remote_w.write(new_first + rest_headers)
            await remote_w.drain()

        # Двунаправленный pipe
        await asyncio.gather(
            pipe(local_r, remote_w),
            pipe(remote_r, local_w),
        )
    except Exception:
        pass
    finally:
        for w in [local_w, remote_w]:
            if w:
                try:
                    w.close()
                except:
                    pass


async def main(listen_port, proxy_url):
    async def handler(r, w):
        await handle_client(r, w, proxy_url)

    server = await asyncio.start_server(handler, '127.0.0.1', listen_port)
    print(f'FORWARDER_READY:{listen_port}', flush=True)
    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    port = int(sys.argv[1])
    proxy_url = sys.argv[2]
    asyncio.run(main(port, proxy_url))
