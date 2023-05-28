/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/
DROP TABLE IF EXISTS data;

CREATE TABLE data (
letra STRING,
fecha STRING,
valor INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY  '\t';

SELECT letra, COUNT(*) cantidad
FROM data
GROUP BY letra;

INSERT OVERWRITE DIRECTORY 'output'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','