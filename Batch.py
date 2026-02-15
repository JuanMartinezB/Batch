# ============================================
# BIG DATA BATCH REAL: CSV -> CHUNKS -> EXCEL
# CON MEDICIÓN DE TIEMPO
# ============================================

import pandas as pd
import random
import time

# ============================================
# PARTE 1: CREAR CSV GRANDE (SIN CARGAR EN RAM)
# ============================================

def crear_csv_grande(nombre_archivo="estudiantes_bigdata.csv", cantidad=500):
    with open(nombre_archivo, "w", encoding="utf-8") as f:
        f.write("ID,Nombre,Nota\n")
        for i in range(1, cantidad + 1):
            nota = round(random.uniform(1.0, 5.0), 2)
            f.write(f"{i},Estudiante_{i},{nota}\n")

    print(f"CSV creado: {nombre_archivo} con {cantidad} registros.")


# ============================================
# PARTE 2: PROCESAMIENTO BATCH REAL CON CHUNKS
# ============================================

TAMANO_LOTE = 50  # tamaño del batch

def procesamiento_batch_bigdata(csv_entrada="estudiantes_bigdata.csv", excel_salida="reporte_batch.xlsx"):
    
    resultados = []
    lote_num = 1

    print("\n=== INICIANDO PROCESAMIENTO BATCH ===")
    inicio = time.time()   # ⏱️ tiempo total inicial

    # Procesar en bloques (BIG DATA REAL)
    for lote in pd.read_csv(csv_entrada, chunksize=TAMANO_LOTE):
        
        inicio_lote = time.time()  # ⏱️ inicio del lote
        
        print(f"\nProcesando lote {lote_num} con {len(lote)} registros...")

        promedio = lote["Nota"].mean()

        fin_lote = time.time()  # ⏱️ fin del lote
        tiempo_lote = fin_lote - inicio_lote

        print(f"Promedio lote {lote_num}: {round(promedio, 2)}")
        print(f"Tiempo lote {lote_num}: {tiempo_lote:.4f} segundos")

        resultados.append({
            "Lote": lote_num,
            "Promedio": round(promedio, 2),
            "Tiempo_segundos": round(tiempo_lote, 4)
        })

        lote_num += 1

    fin = time.time()  # ⏱️ tiempo total final
    tiempo_total = fin - inicio

    # ============================================
    # GUARDAR REPORTE FINAL EN EXCEL
    # ============================================

    df_reporte = pd.DataFrame(resultados)
    df_reporte.to_excel(excel_salida, index=False)

    print(f"\nReporte guardado en: {excel_salida}")
    print(f"Tiempo total de procesamiento: {round(tiempo_total, 4)} segundos")


# ============================================
# MAIN PIPELINE BIG DATA
# ============================================

if __name__ == "__main__":
    print("=== SISTEMA BIG DATA BATCH PROFESIONAL ===")

    # 1. Generar CSV grande
    crear_csv_grande(cantidad=500)   # cambia a 100000 o 1000000 para Big Data real

    # 2. Procesar CSV en batch
    procesamiento_batch_bigdata()

    print("\nProceso Big Data Batch finalizado.")
