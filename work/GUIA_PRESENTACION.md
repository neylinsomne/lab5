# Guía de Presentación - Lab 05: Backup & Restoring

## Slide 1: Portada
- Título: "Lab 05: Backup & Restoring con SQL Server + Iperius Backup"
- Nombre(s), fecha, materia
- Logo PUJ (opcional)

## Slide 2: Introducción y Contexto
- Qué es Backup & Restoring y por qué es crítico en la administración de BD
- Objetivo del lab: simular ingesta de datos en tiempo real, provocar una pérdida catastrófica y recuperar datos
- Herramientas elegidas:
  - **DBMS:** Microsoft SQL Server 2022 (Developer Edition, Docker)
  - **B&R Tool:** SQL Server Native Backup + Iperius Backup (para automatización)
  - **Datos:** SUMO - TAPAS Cologne Scenario

## Slide 3: Arquitectura del Entorno
- Diagrama mostrando:
  - Docker Compose con SQL Server 2022
  - Contenedor Python (ingester) para parsear XML de SUMO
  - SUMO generando datos FCD (Floating Car Data)
  - Backup periódico a directorio /var/opt/backups
- **Captura:** `docker ps` mostrando los contenedores corriendo

## Slide 4: Fuente de Datos - SUMO TAPAS Cologne
- Qué es SUMO (Simulation of Urban MObility)
- Qué es el escenario TAPAS Cologne (simulación de tráfico de la ciudad de Colonia, Alemania)
- Formato de datos: XML FCD con timestep, vehicle_id, x, y, speed, angle, lane
- **Captura:** SUMO corriendo la simulación

## Slide 5: Database Setup (20% de la nota)
- SQL Server 2022 en Docker
- Esquema de la tabla `vehicle_positions`:
  ```
  id (BIGINT, PK, IDENTITY)
  timestep (FLOAT) - segundo de simulación
  vehicle_id (NVARCHAR) - ID del vehículo
  x, y (FLOAT) - coordenadas
  speed (FLOAT) - velocidad
  angle (FLOAT) - ángulo
  lane (NVARCHAR) - carril
  pos (FLOAT) - posición en el carril
  inserted_at (DATETIME2) - timestamp de inserción
  ```
- Recovery model configurado en FULL (necesario para log backups)
- Índice en `timestep` para queries rápidas
- **Captura:** Query mostrando la tabla creada y datos insertándose

## Slide 6: Ingesta de Datos en Tiempo Real
- Script Python con `lxml.etree.iterparse` para parsing streaming del XML
- Inserción en batches de 500 filas para eficiencia
- Monitoreo del progreso por timestep
- **Captura:** Terminal mostrando la ingesta en progreso con conteo de filas

## Slide 7: Estrategia de Backup (25% de la nota)
- **Recovery Model FULL:** permite backups de transaction log + point-in-time restore
- Tipos de backup implementados:
  1. **FULL Backup** (cada 3er ciclo): copia completa de toda la BD
  2. **Transaction Log Backup** (entre FULLs): solo los cambios desde el último backup
- Backups ejecutados cada 2 minutos durante la simulación
- Almacenados en `/var/opt/backups/` (volumen Docker montado)
- **Captura:** Terminal del backup_periodic.py mostrando los backups ejecutándose
- **Captura:** Listado de archivos .bak y .trn generados

## Slide 8: Iperius Backup como Herramienta Complementaria
- Qué es Iperius Backup: herramienta comercial (versión free disponible) para backup de SQL Server
- Soporta: Full, Differential, Transaction Log backups
- Interfaz gráfica para programar backups automáticos
- Ventajas sobre scripts manuales: GUI, scheduling, notificaciones por email, compresión
- **Captura:** Interfaz de Iperius Backup configurada con SQL Server (si lo instalaste)

## Slide 9: Simulación del Evento Catastrófico (15% de la nota)
- Escenario: un usuario/atacante ejecuta un DELETE masivo
- Comando ejecutado:
  ```sql
  DELETE FROM vehicle_positions
  WHERE timestep >= 4000 AND timestep <= 6000
  ```
- Esto elimina todos los datos de vehículos entre los segundos 4000 y 6000 de la simulación
- Equivale a ~2000 segundos de datos de tráfico perdidos
- **Captura:** Output del script mostrando el ANTES (total de filas) y DESPUÉS del DELETE (filas perdidas)

## Slide 10: Proceso de Recuperación (20% de la nota)
- Pasos ejecutados:
  1. Se detecta la pérdida de datos
  2. Se identifica el backup más reciente previo a la catástrofe
  3. Se pone la BD en SINGLE_USER mode
  4. Se ejecuta RESTORE DATABASE desde el backup .bak
  5. Se devuelve la BD a MULTI_USER mode
- Comandos SQL usados:
  ```sql
  ALTER DATABASE [sumo_traffic] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
  RESTORE DATABASE [sumo_traffic] FROM DISK = '...' WITH REPLACE
  ALTER DATABASE [sumo_traffic] SET MULTI_USER
  ```
- **Captura:** Output del script de restore mostrando el proceso completo

## Slide 11: Análisis de Recuperación
- Tabla comparativa:
  | Métrica | Valor |
  |---------|-------|
  | Filas antes de catástrofe | X,XXX |
  | Filas después del DELETE | X,XXX |
  | Filas después del restore | X,XXX |
  | Filas recuperadas | X,XXX |
  | Tasa de recuperación | XX.X% |
- Análisis: ¿se recuperó el 100%? Si no, ¿por qué?
  - Si el backup fue ANTES de que los datos se insertaran, algunos datos post-backup se pierden
  - Esto demuestra la importancia de la FRECUENCIA de backups
- **Captura:** Output final del script con el análisis numérico

## Slide 12: Lecciones Aprendidas y Conclusiones
- La frecuencia del backup determina la ventana máxima de pérdida (RPO - Recovery Point Objective)
- El recovery model FULL es esencial para log backups y point-in-time recovery
- Herramientas como Iperius Backup facilitan la automatización en entornos de producción
- En producción real: combinar full + differential + log backups
- Testear los restores periódicamente (un backup no testeado no es un backup)
- Tener backups offsite (no solo en el mismo servidor)

## Slide 13: Demo en Vivo (Opcional)
- Si el tiempo lo permite: correr el script de catástrofe y restore en vivo
- Mostrar el antes, el DELETE, y la recuperación en tiempo real

## Slide 14: Preguntas
- Espacio para preguntas del público y evaluadores
