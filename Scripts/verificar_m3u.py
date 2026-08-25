#!/usr/bin/env python3
"""
Verificador de listas M3U (IPTV)
---------------------------------
Lee un archivo .m3u/.m3u8, prueba cada stream en paralelo y genera:
  - un .txt con TODOS los canales caídos/con error (agrupados por categoría,
    con nombre, enlace y motivo del fallo)
  - un resumen en consola (y al inicio del .txt) con totales

Uso:
    python verificar_m3u.py teleon_sv.m3u
    python verificar_m3u.py teleon_sv.m3u --salida caidos.txt --hilos 60 --timeout 6

Requisitos:
    pip install requests --break-system-packages
"""

import argparse
import re
import sys
import time
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse

# Forzar salida en UTF-8 y reemplazar caracteres que la consola no pueda
# mostrar (evita el UnicodeEncodeError en Windows con cp1252, tanto en
# pantalla como al redirigir con '>' a un archivo).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from requests.exceptions import (
    ConnectionError as ReqConnectionError,
    SSLError,
    Timeout,
    TooManyRedirects,
    RequestException,
)

# Cabeceras "amigables" para servidores IPTV que bloquean user-agents raros
HEADERS = {
    "User-Agent": "VLC/3.0.20 LibVLC/3.0.20",
    "Accept": "*/*",
    "Connection": "keep-alive",
}

# Session global con pool de conexiones reutilizables (keep-alive). Evita
# rehacer el handshake TCP/TLS en cada intento/reintento del mismo host y
# es la mejora de velocidad más grande para listas con muchos canales del
# mismo servidor. El tamaño del pool se ajusta en main() según --hilos.
_SESSION = requests.Session()


def _limpiar_valor_extvlcopt(valor):
    """Quita comillas envolventes (simples o dobles) y espacios de un valor
    de #EXTVLCOPT. Enviar comillas literales en el header puede causar 403
    en servidores estrictos, aunque el valor "de fondo" sea correcto."""
    valor = valor.strip()
    if len(valor) >= 2 and valor[0] == valor[-1] and valor[0] in ("'", '"'):
        valor = valor[1:-1].strip()
    return valor


def parsear_m3u(ruta):
    """Extrae (nombre, categoria, url, user_agent, referrer) de cada entrada #EXTINF del m3u."""
    canales = []
    with open(ruta, "r", encoding="utf-8", errors="ignore") as f:
        lineas = [l.strip() for l in f if l.strip()]

    i = 0
    while i < len(lineas):
        linea = lineas[i]
        if linea.startswith("#EXTINF"):
            nombre = linea.split(",", 1)[-1].strip() if "," in linea else "Sin nombre"

            m_grupo = re.search(r'group-title="([^"]*)"', linea)
            categoria = m_grupo.group(1) if m_grupo else "Sin categoría"

            # recorrer líneas siguientes: pueden venir #EXTVLCOPT (user-agent/referrer)
            # antes de llegar a la URL real
            j = i + 1
            url = None
            user_agent = None
            referrer = None
            while j < len(lineas):
                l = lineas[j]
                if l.startswith("#EXTVLCOPT:http-user-agent="):
                    user_agent = _limpiar_valor_extvlcopt(l.split("=", 1)[1])
                    j += 1
                    continue
                if l.startswith("#EXTVLCOPT:http-referrer=") or l.startswith("#EXTVLCOPT:http-referer="):
                    referrer = _limpiar_valor_extvlcopt(l.split("=", 1)[1])
                    j += 1
                    continue
                if l.startswith("#"):
                    j += 1
                    continue
                url = l
                break

            if url:
                canales.append({
                    "nombre": nombre,
                    "categoria": categoria,
                    "url": url,
                    "user_agent": user_agent,
                    "referrer": referrer,
                })
            i = j + 1
        else:
            i += 1

    return canales


def clasificar_error(exc):
    """Convierte la excepción de requests en una categoría de error legible."""
    if isinstance(exc, Timeout):
        return "TIMEOUT (no respondió a tiempo)"
    if isinstance(exc, SSLError):
        return "ERROR SSL/TLS"
    if isinstance(exc, TooManyRedirects):
        return "DEMASIADAS REDIRECCIONES"
    if isinstance(exc, ReqConnectionError):
        return "ERROR DE CONEXIÓN (servidor caído / DNS / rechazado)"
    if isinstance(exc, RequestException):
        return f"ERROR DE RED ({type(exc).__name__})"
    return f"ERROR DESCONOCIDO ({type(exc).__name__})"


def evaluar_contenido(resp, url):
    """
    Revisa que la respuesta sea realmente un stream/manifiesto válido y no,
    por ejemplo, una página de error en HTML disfrazada con status 200.
    Devuelve (es_valido, motivo_si_no_es_valido).
    """
    content_type = resp.headers.get("Content-Type", "").lower()

    es_manifiesto = url.lower().split("?")[0].endswith((".m3u8", ".m3u"))

    if es_manifiesto:
        # Para manifiestos HLS, el cuerpo debe empezar con #EXTM3U
        try:
            inicio = next(resp.iter_content(chunk_size=256), b"")
        except Exception:
            inicio = b""
        texto = inicio.decode("utf-8", errors="ignore").strip()

        if texto.startswith("#EXTM3U"):
            return True, None
        if "<html" in texto.lower() or "<!doctype" in texto.lower():
            return False, "responde 200 pero devuelve una página HTML (no el manifiesto)"
        if not texto:
            return False, "responde 200 pero el cuerpo llegó vacío"
        return False, "responde 200 pero el contenido no parece un manifiesto M3U válido"

    # Para streams directos (.ts, .mp4, etc.) validamos por Content-Type
    if content_type and not any(t in content_type for t in ("video", "mpegurl", "octet-stream", "mp2t")):
        if "html" in content_type or "text/plain" in content_type:
            return False, f"responde 200 pero Content-Type es '{content_type}' (no es video)"

    return True, None


# Semáforos por servidor (host:puerto) para no abrir más de N conexiones
# simultáneas contra el mismo panel/servidor. Muchos paneles Xtream tienen
# un límite de "líneas simultáneas" por cuenta y, al excederlo, responden
# 404 en vez de un código más claro como 503 -- eso hace que canales que sí
# están en línea se marquen como caídos solo por la concurrencia del check.
_semaforos_lock = threading.Lock()
_semaforos_por_host = {}


def _semaforo_de(url, max_conexiones):
    host = urlparse(url).netloc
    with _semaforos_lock:
        sem = _semaforos_por_host.get(host)
        if sem is None:
            sem = threading.Semaphore(max_conexiones)
            _semaforos_por_host[host] = sem
    return sem


def _intentar_una_vez(url, headers, timeout, verify=True):
    """Un solo intento de request. Devuelve (estado, error, codigo_http).

    El timeout se pasa como (timeout_conexion, timeout_lectura): el de
    conexión se recorta para no perder tiempo con servidores que ni
    siquiera responden al handshake TCP/TLS.
    """
    timeout_conexion = min(4, timeout)
    resp = _SESSION.get(
        url, headers=headers, timeout=(timeout_conexion, timeout),
        stream=True, allow_redirects=True, verify=verify,
    )
    codigo = resp.status_code
    if codigo >= 400:
        resp.close()
        return None, f"HTTP {codigo}", codigo
    valido, motivo = evaluar_contenido(resp, url)
    resp.close()
    if valido:
        return "OK", None, codigo
    return None, f"CONTENIDO INVÁLIDO ({motivo})", codigo


def verificar_canal(canal, timeout, max_conexiones_por_servidor=1, reintentos=2, espera_reintento=1.5):
    """Devuelve el canal con 'estado' y 'error' (error=None si está OK).

    Aplica un límite de conexiones simultáneas por servidor y reintenta
    antes de marcar un canal como caído, para evitar falsos 404 causados
    por límites de conexión del panel o bloqueos temporales por ráfaga
    de solicitudes.
    """
    url = canal["url"]

    # headers propios del canal (si el m3u trae #EXTVLCOPT) tienen prioridad
    tiene_ua_propio = bool(canal.get("user_agent"))
    headers = dict(HEADERS)
    if tiene_ua_propio:
        headers["User-Agent"] = canal["user_agent"]
    if canal.get("referrer"):
        headers["Referer"] = canal["referrer"]

    sem = _semaforo_de(url, max_conexiones_por_servidor)

    ultimo_error = None
    with sem:
        for intento in range(reintentos + 1):
            fue_error_de_conexion = False
            try:
                estado, error, codigo = _intentar_una_vez(url, headers, timeout)
                if estado == "OK":
                    canal["estado"] = "OK"
                    canal["error"] = None
                    return canal

                ultimo_error = error
            except SSLError:
                try:
                    estado, error, codigo = _intentar_una_vez(url, headers, timeout, verify=False)
                    if estado == "OK":
                        canal["estado"] = "OK (SSL inválido, pero responde)"
                        canal["error"] = None
                        return canal
                    ultimo_error = f"{error} (con SSL inválido)"
                except Exception as e2:
                    ultimo_error = clasificar_error(e2)
                    fue_error_de_conexion = isinstance(e2, ReqConnectionError)
            except ReqConnectionError as e:
                # Servidor caído/DNS/rechazado: reintentar con espera no
                # suele arreglarlo, así que aquí no vale la pena perder
                # tiempo -- se corta sin la pausa de espera_reintento.
                ultimo_error = clasificar_error(e)
                fue_error_de_conexion = True
            except Exception as e:
                ultimo_error = clasificar_error(e)

            # Si no fue el último intento y no fue un error de conexión
            # "definitivo", espera un poco (da tiempo a que se libere una
            # conexión en el servidor o pase el bloqueo temporal).
            if intento < reintentos and not fue_error_de_conexion:
                time.sleep(espera_reintento)
            elif fue_error_de_conexion:
                break

    canal["estado"] = "ERROR"
    canal["error"] = ultimo_error
    return canal


def main():
    ap = argparse.ArgumentParser(description="Verificador de listas M3U/IPTV")
    ap.add_argument("archivo", help="Ruta del archivo .m3u a verificar")
    ap.add_argument("--salida", default="canales_caidos.txt", help="Nombre del .txt de salida")
    ap.add_argument("--hilos", type=int, default=50, help="Cantidad de hilos en paralelo (default 50)")
    ap.add_argument("--timeout", type=int, default=6, help="Timeout de lectura por canal en segundos (default 6)")
    ap.add_argument("--max-por-servidor", type=int, default=1,
                     help="Máximo de conexiones simultáneas contra un mismo servidor/host (default 1)")
    ap.add_argument("--reintentos", type=int, default=1,
                     help="Reintentos antes de marcar un canal como caído (default 1)")
    ap.add_argument("--espera-reintento", type=float, default=1.0,
                     help="Segundos de espera entre reintentos (default 1.0)")
    args = ap.parse_args()

    print(f"[+] Leyendo lista: {args.archivo}")
    canales = parsear_m3u(args.archivo)
    total = len(canales)
    print(f"[+] Canales encontrados: {total}")

    if total == 0:
        print("[!] No se encontraron canales. Verifica el formato del archivo.")
        sys.exit(1)

    # Ajustar el pool de conexiones de la Session al número de hilos, para
    # que cada hilo pueda reutilizar su propia conexión keep-alive.
    adapter = requests.adapters.HTTPAdapter(pool_connections=args.hilos, pool_maxsize=args.hilos)
    _SESSION.mount("http://", adapter)
    _SESSION.mount("https://", adapter)

    print(f"[+] Verificando con {args.hilos} hilos, timeout {args.timeout}s por canal...\n")

    resultados = []
    completados = 0
    with ThreadPoolExecutor(max_workers=args.hilos) as ex:
        futuros = {
            ex.submit(
                verificar_canal, c, args.timeout,
                args.max_por_servidor, args.reintentos, args.espera_reintento
            ): c
            for c in canales
        }
        for fut in as_completed(futuros):
            r = fut.result()
            resultados.append(r)
            completados += 1
            estado_txt = "OK" if r["estado"].startswith("OK") else "CAÍDO"
            print(f"[{completados}/{total}] {estado_txt:6} - {r['nombre']} ({r['categoria']})")

    caidos = [r for r in resultados if r["estado"] == "ERROR"]
    ok = [r for r in resultados if r["estado"].startswith("OK")]

    # Agrupar caídos por categoría
    por_categoria = {}
    for c in caidos:
        por_categoria.setdefault(c["categoria"], []).append(c)

    with open(args.salida, "w", encoding="utf-8") as f:
        f.write("REPORTE DE VERIFICACIÓN M3U\n")
        f.write(f"Archivo analizado: {args.archivo}\n")
        f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Total canales: {total}\n")
        f.write(f"OK: {len(ok)}   |   Caídos/Error: {len(caidos)}\n")
        f.write("=" * 60 + "\n\n")

        if not caidos:
            f.write("¡Todos los canales respondieron correctamente!\n")
        else:
            for categoria in sorted(por_categoria.keys()):
                lista = por_categoria[categoria]
                f.write(f"=== CATEGORÍA: {categoria} ({len(lista)} caídos) ===\n\n")
                for c in lista:
                    f.write(f"Nombre : {c['nombre']}\n")
                    f.write(f"Error  : {c['error']}\n")
                    f.write(f"Enlace : {c['url']}\n")
                    f.write("-" * 50 + "\n")
                f.write("\n")

    print(f"\n[+] Listo. Reporte guardado en: {args.salida}")
    print(f"[+] Resumen -> OK: {len(ok)}  |  Caídos: {len(caidos)} de {total}")


if __name__ == "__main__":
    main()
