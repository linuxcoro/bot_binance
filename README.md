# Scraper de Bots de Binance

Este script permite extraer información de bots de futuros en Binance, aplicando filtros avanzados y optimizaciones para un scraping eficiente.

## Requisitos
- Python 3.8+
- Playwright (async)
- pandas
- tabulate

Instala dependencias con:
```bash
pip install playwright pandas tabulate
playwright install
```

## Uso básico

```bash
python scraper.py [opciones]
```

## Filtros disponibles

- `--direccion`           : Filtra por dirección del bot (`neutral`, `long`, `short`).
- `--inversion-max`       : Monto máximo de inversión en USDT (ej: 200).
- `--copias-min`          : Número mínimo de copias activas (ej: 10).
- `--copias-max`          : Número máximo de copias activas.
- `--paginas-max`         : Número máximo de páginas a recorrer (por defecto: todas).

Puedes combinar varios filtros a la vez.

## Ejemplos de uso

- Operaciones neutrales, inversión máxima 200 USDT, mínimo 10 copias:
  ```bash
  python scraper.py --direccion neutral --inversion-max 200 --copias-min 10
  ```

- Solo bots con más de 50 copias y dirección long:
  ```bash
  python scraper.py --direccion long --copias-min 50
  ```

- Limitar a 3 páginas de resultados:
  ```bash
  python scraper.py --paginas-max 3
  ```

## Salida

El script muestra los resultados en formato tabular en consola, indicando los filtros activos.

## Optimización
- El script bloquea imágenes, videos y fuentes para acelerar la carga.
- Navega automáticamente por la paginación de la web.

## Notas
- Si la web cambia, puede requerir ajustes en el scraping.
- Para dudas o mejoras, revisa el código fuente o consulta al desarrollador.
