# ⚡ Análise e Processamento de Dados de Consumo Elétrico com PySpark

Este projeto aplica técnicas de **Big Data e Engenharia de Dados** utilizando **PySpark e Python** para o processamento e análise de métricas operacionais de consumo elétrico provenientes de medidores inteligentes (*Smart Meters*).

---

## 🎯 Objetivo do Projeto
Processar e transformar grandes volumes de dados de telemetria elétrica, identificando automaticamente:
1. Períodos de maior demanda e consumo no **Horário de Pico** (18h às 21h).
2. Ocorrências de **baixo fator de potência** (< 0.92), sinalizando necessidade de correção de reativos na rede.
3. Métricas consolidadas de tensão e consumo por unidade consumidora.

---

## 🛠️ Tecnologias Utilizadas
* **Python 3:** Lógica de scripts de análise de dados.
* **PySpark (Apache Spark):** Manipulação e processamento distribuído de dataframes em larga escala.
* **Engenharia de Features:** Criação de colunas condicionais (`withColumn`, `when`), agregadores e filtros de rede elétrica.

---

## 📐 Estrutura das Variáveis
* `medidor_id`: Identificador único do ponto de medição.
* `timestamp`: Data e hora do registro de telemetria.
* `consumo_kwh`: Consumo ativo apurado na medição (kWh).
* `tensao_volts`: Tensão eficaz da fase (V).
* `fator_potencia`: Fator de potência registrado no intervalo.

---

## 👩‍💻 Autora
**Mariane Aguiar**  
*Engenheira | Analista de Planejamento e Dados*  
[LinkedIn](https://www.linkedin.com/in/mariane-de-aguiar)
