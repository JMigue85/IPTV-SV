#!/usr/bin/env python3
"""
Genera REPORTE.md a partir de los archivos .m3u del repositorio,
reutilizando la lógica de verificar_m3u.py (parseo + verificación).

No modifica verificar_m3u.py: solo importa sus funciones.

Uso:
    python generar_reporte.py IPTVSV.m3u PlutoTV.ES.m3u PlutoTV.MX.m3u
"""

import sys
import argparse
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

from verificar_m3u import parsear_m3u, verificar_canal

# Códigos HTTP que casi siempre son geo-bloqueo / restricción, no un canal
# realmente caído. Los marcamos aparte para no generar alarma falsa.
CODIGOS_POSIBLE_FALSO_POSITIVO = ("HTTP 403", "HTTP 451", "HTTP 401")

# Etiquetas que el propio dueño de la lista ya usa en el nombre del canal
# para marcar restricción geográfica. Si el canal trae esta etiqueta, se
# marca como posible falso positivo sin importar qué error haya dado
# (timeout, conexión rechazada, etc.) -- porque el canal casi seguro sí
# funciona para usuarios en la región correcta.
ETIQUETAS_GEO_EN_NOMBRE = ("geo-blocked", "geo blocked", "geobloqueado", "geo-bloqueado", "geo bloqueado")


def es_posible_falso_positivo(error, nombre=""):
    if nombre and any(etiqueta in nombre.lower() for etiqueta in ETIQUETAS_GEO_EN_NOMBRE):
        return True
    if not error:
        return False
    return any(codigo in error for codigo in CODIGOS_POSIBLE_FALSO_POSITIVO)


def verificar_archivo(ruta, hilos, timeout, max_por_servidor, reintentos, espera_reintento):
    print(f"\n[+] Procesando {ruta}...")
    canales = parsear_m3u(ruta)
    total = len(canales)
    print(f"    Canales encontrados: {total}")

    if total == 0:
        return []

    resultados = []
    with ThreadPoolExecutor(max_workers=hilos) as ex:
        futuros = {
            ex.submit(
                verificar_canal, c, timeout, max_por_servidor, reintentos, espera_reintento
            ): c
            for c in canales
        }
        completados = 0
        for fut in as_completed(futuros):
            r = fut.result()
            resultados.append(r)
            completados += 1
            if completados % 50 == 0 or completados == total:
                print(f"    [{completados}/{total}] verificados...")

    return resultados


def escribir_reporte(resultados_por_archivo, ruta_salida):
    fecha = datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC")

    total_general = sum(len(r) for r in resultados_por_archivo.values())
    ok_general = sum(
        len([c for c in r if c["estado"].startswith("OK")])
        for r in resultados_por_archivo.values()
    )
    caidos_general = total_general - ok_general

    lineas = []
    lineas.append("# 📡 Reporte de estado de canales\n")
    lineas.append(f"**Última verificación:** {fecha}\n")
    lineas.append(
        "> ⚠️ **Nota importante:** este reporte se genera automáticamente desde "
        "servidores de GitHub Actions (ubicados en EE.UU./Europa). Un canal puede "
        "aparecer como caído sin estarlo realmente para el usuario final, por:\n"
        ">\n"
        "> - **Geo-bloqueo** (el canal solo permite IPs de cierto país/región)\n"
        "> - **Restricción de User-Agent** (el servidor solo acepta reproductores específicos)\n"
        "> - **Restricción de Referer** (el servidor exige que la petición venga de cierto sitio)\n"
        ">\n"
        "> Los errores marcados con 🟡 **posible falso positivo** son los más propensos "
        "a este tipo de bloqueo -- ya sea por el código de respuesta (HTTP 401/403/451) "
        "o porque el propio canal ya trae la etiqueta **[Geo-Blocked]** en su nombre -- "
        "y no implican necesariamente que el canal esté realmente caído.\n"
    )
    lineas.append("## Resumen general\n")
    lineas.append("| Total canales | ✅ OK | ❌ Caídos/Error |")
    lineas.append("|---|---|---|")
    lineas.append(f"| {total_general} | {ok_general} | {caidos_general} |\n")

    for archivo, resultados in resultados_por_archivo.items():
        total = len(resultados)
        ok = [c for c in resultados if c["estado"].startswith("OK")]
        caidos = [c for c in resultados if c["estado"] == "ERROR"]

        lineas.append(f"## {archivo}\n")
        lineas.append(f"**Total:** {total} &nbsp;|&nbsp; **OK:** {len(ok)} &nbsp;|&nbsp; **Caídos:** {len(caidos)}\n")

        if not caidos:
            lineas.append("✅ Todos los canales respondieron correctamente.\n")
            continue

        por_categoria = {}
        for c in caidos:
            por_categoria.setdefault(c["categoria"], []).append(c)

        for categoria in sorted(por_categoria.keys()):
            lista = por_categoria[categoria]
            lineas.append(f"<details>\n<summary><strong>{categoria}</strong> ({len(lista)} caídos)</summary>\n")
            lineas.append("| Canal | Motivo |")
            lineas.append("|---|---|")
            for c in lista:
                marca = "🟡 *(posible falso positivo)*" if es_posible_falso_positivo(c["error"], c["nombre"]) else "🔴"
                lineas.append(f"| {c['nombre']} | {marca} {c['error']} |")
            lineas.append("\n</details>\n")

    with open(ruta_salida, "w", encoding="utf-8") as f:
        f.write("\n".join(lineas))

    print(f"\n[+] Reporte guardado en: {ruta_salida}")
    print(f"[+] Resumen -> OK: {ok_general}  |  Caídos: {caidos_general} de {total_general}")


def main():
    ap = argparse.ArgumentParser(description="Genera REPORTE.md a partir de varios .m3u")
    ap.add_argument("archivos", nargs="+", help="Rutas de los .m3u a verificar")
    ap.add_argument("--salida", default="REPORTE.md", help="Archivo Markdown de salida")
    ap.add_argument("--hilos", type=int, default=50)
    ap.add_argument("--timeout", type=int, default=6)
    ap.add_argument("--max-por-servidor", type=int, default=1)
    ap.add_argument("--reintentos", type=int, default=1)
    ap.add_argument("--espera-reintento", type=float, default=1.0)
    args = ap.parse_args()

    adapter = requests.adapters.HTTPAdapter(pool_connections=args.hilos, pool_maxsize=args.hilos)
    import verificar_m3u
    verificar_m3u._SESSION.mount("http://", adapter)
    verificar_m3u._SESSION.mount("https://", adapter)

    resultados_por_archivo = {}
    for archivo in args.archivos:
        resultados_por_archivo[archivo] = verificar_archivo(
            archivo, args.hilos, args.timeout, args.max_por_servidor,
            args.reintentos, args.espera_reintento,
        )

    escribir_reporte(resultados_por_archivo, args.salida)


if __name__ == "__main__":
    main()
