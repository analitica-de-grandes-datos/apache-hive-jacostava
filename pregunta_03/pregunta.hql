/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Escriba una consulta que devuelva los cinco valores diferentes más pequeños 
de la tercera columna.

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/
u = LOAD 'data.tsv' USING PigStorage('\t')
        AS(letra:CHARARRAY,
        fecha:CHARARRAY,
        numero:INT);

y = ORDER u BY numero;
z = FOREACH y GENERATE $2;
z = LIMIT z 5;
STORE z INTO 'output' USING PigStorage(',');

