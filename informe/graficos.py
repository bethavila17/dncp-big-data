"""Gráficos del informe a partir de los exportables de Spark (verificados en 06_verificar.py).

Uso:  python informe/graficos.py   ->  informe/graficos/*.png
"""
import os

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ALCANCE = os.environ.get("ALCANCE", "full")
SPARK = os.path.join(RAIZ, "resultados", ALCANCE, "spark")
SALIDA = os.path.join(RAIZ, "informe", "graficos")
os.makedirs(SALIDA, exist_ok=True)

AZUL, NARANJA = "#2a78d6", "#eb6834"
TEXTO, TEXTO_2, GRILLA = "#0b0b0b", "#52514e", "#e4e3df"
plt.rcParams.update({
    "font.family": "Arial", "font.size": 9, "text.color": TEXTO, "axes.labelcolor": TEXTO_2,
    "xtick.color": TEXTO_2, "ytick.color": TEXTO_2, "axes.edgecolor": GRILLA,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True, "grid.color": GRILLA,
    "grid.linewidth": 0.6, "axes.axisbelow": True, "figure.dpi": 200, "savefig.bbox": "tight",
})


def leer(nombre):
    return pd.read_csv(os.path.join(SPARK, f"{nombre}.csv"))


def miles(x):
    return f"{x:,.0f}".replace(",", ".")


def dec(x, d=1):
    """Decimal con coma, como en el texto del informe."""
    return f"{x:.{d}f}".replace(".", ",")


def guardar(fig, nombre):
    fig.savefig(os.path.join(SALIDA, f"{nombre}.png"), facecolor="white")
    plt.close(fig)
    print(f"informe/graficos/{nombre}.png")


def g1_evolucion():
    m = leer("p1_anio_modalidad")
    m = m[m["anio"].astype(str).str.isdigit()]
    m = m.assign(anio=m["anio"].astype(int),
                 llamados=m["procesos"].where(m["modalidad"] != "(sin dato)", 0))
    d = m.groupby("anio")[["llamados", "monto_adjudicado_pyg"]].sum().reset_index()
    d = d[(d["anio"] >= 2011) & (d["anio"] <= 2025)]
    fig, (a, b) = plt.subplots(1, 2, figsize=(7.2, 2.6))
    a.bar(d["anio"], d["llamados"] / 1000, color=AZUL, width=0.7)
    a.set_title("Llamados publicados por año, en miles", loc="left", fontsize=9)
    b.bar(d["anio"], d["monto_adjudicado_pyg"] / 1e12, color=AZUL, width=0.7)
    b.set_title("Monto adjudicado por año, billones de PYG", loc="left", fontsize=9)
    for ax in (a, b):
        ax.set_xticks(d["anio"][::2])
        ax.grid(axis="x", visible=False)
        ax.tick_params(length=0)
    guardar(fig, "g1_evolucion_anual")


def g2_unica_oferta():
    d = leer("p2_unica_oferta_tipo_entidad").sort_values("pct_una_oferta")
    fig, ax = plt.subplots(figsize=(7.2, 3.6))
    barras = ax.barh(d["tipo_entidad"], d["pct_una_oferta"], color=AZUL, height=0.65)
    for barra, pct, n in zip(barras, d["pct_una_oferta"], d["llamados_con_oferta"]):
        ax.text(barra.get_width() + 0.6, barra.get_y() + barra.get_height() / 2,
                f"{dec(pct)}%  ({miles(n)} llamados)", va="center", fontsize=7.5, color=TEXTO_2)
    ax.set_xlim(0, d["pct_una_oferta"].max() * 1.35)
    ax.set_xlabel("% de llamados con una sola oferta")
    ax.grid(axis="y", visible=False)
    ax.tick_params(length=0)
    guardar(fig, "g2_unica_oferta_tipo_entidad")


def g3_plazos():
    d = leer("p6_tramos_dias_oferentes")
    etiquetas = [t.lstrip("0") if t[0] == "0" and t[1] != "-" else t for t in d["tramo_dias"]]
    fig, (a, b) = plt.subplots(1, 2, figsize=(7.2, 2.6))
    a.bar(etiquetas, d["pct_una_oferta"], color=AZUL, width=0.6)
    a.set_title("% de llamados con una sola oferta", loc="left", fontsize=9)
    b.bar(etiquetas, d["oferentes_promedio"], color=AZUL, width=0.6)
    b.set_title("Oferentes promedio por llamado", loc="left", fontsize=9)
    for ax, col, fmt in [(a, "pct_una_oferta", lambda v: f"{v:.0f}%"), (b, "oferentes_promedio", dec)]:
        for x, v in zip(etiquetas, d[col]):
            ax.text(x, v, fmt(v), ha="center", va="bottom", fontsize=7.5, color=TEXTO_2)
        ax.set_xlabel("Días de convocatoria")
        ax.grid(axis="x", visible=False)
        ax.tick_params(length=0)
    guardar(fig, "g3_plazo_vs_oferentes")


def g4_texto_vs_catalogo():
    t = leer("p7_texto_resumen").set_index("grupo")
    c = leer("p7_catalogo_resumen").set_index("grupo")
    grupos = ["combustible", "medicamentos", "alimentos"]
    x = range(len(grupos))
    fig, ax = plt.subplots(figsize=(7.2, 2.8))
    ancho = 0.36
    b1 = ax.bar([i - ancho / 2 - 0.01 for i in x], t.loc[grupos, "monto_pyg"] / 1e12, ancho,
                color=AZUL, label="Descripción menciona la palabra (Elasticsearch / regex)")
    b2 = ax.bar([i + ancho / 2 + 0.01 for i in x], c.loc[grupos, "monto_pyg"] / 1e12, ancho,
                color=NARANJA, label="Clasificación UNSPSC del catálogo DNCP")
    for barras in (b1, b2):
        for b in barras:
            ax.text(b.get_x() + b.get_width() / 2, b.get_height(), dec(b.get_height(), 2),
                    ha="center", va="bottom", fontsize=7.5, color=TEXTO_2)
    ax.set_xticks(list(x), ["Combustible", "Medicamentos", "Alimentos"])
    ax.set_ylabel("Billones de PYG")
    ax.set_ylim(0, max(c.loc[grupos, "monto_pyg"]) / 1e12 * 1.12)
    ax.legend(frameon=False, fontsize=7.5, loc="lower left", bbox_to_anchor=(0, 1.0), ncol=2)
    ax.grid(axis="x", visible=False)
    ax.tick_params(length=0)
    guardar(fig, "g4_texto_vs_catalogo")


if __name__ == "__main__":
    g1_evolucion()
    g2_unica_oferta()
    g3_plazos()
    g4_texto_vs_catalogo()
