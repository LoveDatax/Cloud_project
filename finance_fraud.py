{
  "cells": [
    {
      "cell_type": "markdown",
      "metadata": {
        "id": "view-in-github",
        "colab_type": "text"
      },
      "source": [
        "<a href=\"https://colab.research.google.com/github/LoveDatax/Cloud_project/blob/main/finance_fraud.py\" target=\"_parent\"><img src=\"https://colab.research.google.com/assets/colab-badge.svg\" alt=\"Open In Colab\"/></a>"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "ZQaR3Y09O_sd"
      },
      "outputs": [],
      "source": [
        "from pyspark.sql import SparkSession\n",
        "from pyspark.sql.functions import col, upper, trim, regexp_replace, when, sum, count\n",
        "from pyspark.sql.types import DecimalType"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "if9S94tJPqR1"
      },
      "outputs": [],
      "source": [
        "spark=SparkSession\\\n",
        ".builder\\\n",
        ".appName('Cloud Project')\\\n",
        ".getOrCreate()"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "AO6si0CeP3Iq"
      },
      "outputs": [],
      "source": [
        "finance_fraud=spark.read.csv('/content/drive/MyDrive/Data/synthetic_fraud_dataset.csv',\n",
        "                          header=True,\n",
        "                          inferSchema=True,\n",
        "                          sep=',',\n",
        "                          quote='\"',\n",
        "                          escape='\"',\n",
        "                          multiLine=True,\n",
        "                          mode='PERMISSIVE')"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "8nUY8Tts3Ry0",
        "outputId": "c871c06f-4bf6-45b5-fe96-bbcf0b6acab0"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------+-------+------------------+----------------+-----------------+-------+----+-------------------+--------------------+--------+\n",
            "|transaction_id|user_id|            amount|transaction_type|merchant_category|country|hour|  device_risk_score|       ip_risk_score|is_fraud|\n",
            "+--------------+-------+------------------+----------------+-----------------+-------+----+-------------------+--------------------+--------+\n",
            "|          9608|    363|4922.5875423114285|             ATM|           Travel|     TR|  12| 0.9923465237053082|  0.9479076765522912|       1|\n",
            "|           456|    692| 48.01830285876102|              QR|             Food|     US|  21|0.16857123943741714| 0.22405740349119166|       0|\n",
            "|          4747|    587|136.88196000299405|          Online|           Travel|     TR|  14|0.29612724879473956| 0.12505769241015305|       0|\n",
            "|          6934|    445| 80.53471896232148|             POS|         Clothing|     TR|  23|0.12480097836782779|  0.1592425051585693|       0|\n",
            "|          1646|    729|120.04115818676428|          Online|          Grocery|     FR|  16| 0.0981289537116425| 0.02754196922389681|       0|\n",
            "|          2183|    944| 97.10862458470542|             POS|         Clothing|     DE|  17| 0.2353994323859285|  0.1054540510901703|       0|\n",
            "|          1919|    829| 166.2092618572779|          Online|           Travel|     UK|  12|0.11590590864268228| 0.22371784341004333|       0|\n",
            "|          3479|    845| 96.51263663096326|          Online|          Grocery|     US|   7|0.08225043328647746|0.034023010133471865|       0|\n",
            "|          6796|    129| 83.33870063187989|              QR|             Food|     DE|  16| 0.0217744413637976| 0.27959750653342147|       0|\n",
            "|          5129|    249| 89.69573134806983|              QR|          Grocery|     UK|   6|0.09535343717269791| 0.13633641340833455|       0|\n",
            "|          7402|    701|101.40382672472745|              QR|           Travel|     FR|   7|0.16479393481940688| 0.29193962463857537|       0|\n",
            "|          6188|    116|106.56344355857529|              QR|             Food|     DE|  23|0.13623596858798723|  0.1358305529043107|       0|\n",
            "|          8538|    679| 55.36360144813594|          Online|             Food|     US|  10|0.07430426366719887|  0.1287602546987983|       0|\n",
            "|          8410|    559| 99.41209708030595|          Online|             Food|     DE|  10|0.08477355164396096|0.014333688637872632|       0|\n",
            "|          8201|    564|  94.5456051441663|             ATM|         Clothing|     TR|  20|0.13348972740002596| 0.16200983448572015|       0|\n",
            "|          2187|    336| 133.4106627218069|             POS|             Food|     UK|  19|0.08313216365193023| 0.28602018920359074|       0|\n",
            "|          5666|    166|157.99812334207053|              QR|      Electronics|     FR|  19| 0.2075330047227872|0.019961657221260854|       0|\n",
            "|          1419|    288|202.99589542072368|              QR|          Grocery|     TR|  23|0.15654429930596844| 0.08275756652907204|       0|\n",
            "|          6484|    984| 88.50981483421131|              QR|           Travel|     UK|  19|0.14780302978986357| 0.20648020390836644|       0|\n",
            "|          2480|    137|115.30655945024978|          Online|             Food|     FR|  14|0.20638437804657292| 0.06876635331550568|       0|\n",
            "+--------------+-------+------------------+----------------+-----------------+-------+----+-------------------+--------------------+--------+\n",
            "only showing top 20 rows\n"
          ]
        }
      ],
      "source": [
        "finance_fraud.show()"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "O47uyoAr3fb1",
        "outputId": "586b93eb-5e11-4703-b5eb-7010af9919d1"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "root\n",
            " |-- transaction_id: integer (nullable = true)\n",
            " |-- user_id: integer (nullable = true)\n",
            " |-- amount: double (nullable = true)\n",
            " |-- transaction_type: string (nullable = true)\n",
            " |-- merchant_category: string (nullable = true)\n",
            " |-- country: string (nullable = true)\n",
            " |-- hour: integer (nullable = true)\n",
            " |-- device_risk_score: double (nullable = true)\n",
            " |-- ip_risk_score: double (nullable = true)\n",
            " |-- is_fraud: integer (nullable = true)\n",
            "\n"
          ]
        }
      ],
      "source": [
        "finance_fraud.printSchema()"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "M2Rs9zBO57FK",
        "outputId": "99617214-9f1c-4b8e-cd66-4165f45ac881"
      },
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------+-------+------------------+----------------+-----------------+-------+----+-------------------+--------------------+--------+\n",
            "|transaction_id|user_id|            amount|transaction_type|merchant_category|country|hour|  device_risk_score|       ip_risk_score|is_fraud|\n",
            "+--------------+-------+------------------+----------------+-----------------+-------+----+-------------------+--------------------+--------+\n",
            "|          9608|    363|4922.5875423114285|             ATM|           Travel|     TR|  12| 0.9923465237053082|  0.9479076765522912|       1|\n",
            "|           456|    692| 48.01830285876102|              QR|             Food|     US|  21|0.16857123943741714| 0.22405740349119166|       0|\n",
            "|          4747|    587|136.88196000299405|          Online|           Travel|     TR|  14|0.29612724879473956| 0.12505769241015305|       0|\n",
            "|          6934|    445| 80.53471896232148|             POS|         Clothing|     TR|  23|0.12480097836782779|  0.1592425051585693|       0|\n",
            "|          1646|    729|120.04115818676428|          Online|          Grocery|     FR|  16| 0.0981289537116425| 0.02754196922389681|       0|\n",
            "|          2183|    944| 97.10862458470542|             POS|         Clothing|     DE|  17| 0.2353994323859285|  0.1054540510901703|       0|\n",
            "|          1919|    829| 166.2092618572779|          Online|           Travel|     UK|  12|0.11590590864268228| 0.22371784341004333|       0|\n",
            "|          3479|    845| 96.51263663096326|          Online|          Grocery|     US|   7|0.08225043328647746|0.034023010133471865|       0|\n",
            "|          6796|    129| 83.33870063187989|              QR|             Food|     DE|  16| 0.0217744413637976| 0.27959750653342147|       0|\n",
            "|          5129|    249| 89.69573134806983|              QR|          Grocery|     UK|   6|0.09535343717269791| 0.13633641340833455|       0|\n",
            "|          7402|    701|101.40382672472745|              QR|           Travel|     FR|   7|0.16479393481940688| 0.29193962463857537|       0|\n",
            "|          6188|    116|106.56344355857529|              QR|             Food|     DE|  23|0.13623596858798723|  0.1358305529043107|       0|\n",
            "|          8538|    679| 55.36360144813594|          Online|             Food|     US|  10|0.07430426366719887|  0.1287602546987983|       0|\n",
            "|          8410|    559| 99.41209708030595|          Online|             Food|     DE|  10|0.08477355164396096|0.014333688637872632|       0|\n",
            "|          8201|    564|  94.5456051441663|             ATM|         Clothing|     TR|  20|0.13348972740002596| 0.16200983448572015|       0|\n",
            "|          2187|    336| 133.4106627218069|             POS|             Food|     UK|  19|0.08313216365193023| 0.28602018920359074|       0|\n",
            "|          5666|    166|157.99812334207053|              QR|      Electronics|     FR|  19| 0.2075330047227872|0.019961657221260854|       0|\n",
            "|          1419|    288|202.99589542072368|              QR|          Grocery|     TR|  23|0.15654429930596844| 0.08275756652907204|       0|\n",
            "|          6484|    984| 88.50981483421131|              QR|           Travel|     UK|  19|0.14780302978986357| 0.20648020390836644|       0|\n",
            "|          2480|    137|115.30655945024978|          Online|             Food|     FR|  14|0.20638437804657292| 0.06876635331550568|       0|\n",
            "+--------------+-------+------------------+----------------+-----------------+-------+----+-------------------+--------------------+--------+\n",
            "only showing top 20 rows\n"
          ]
        }
      ],
      "source": [
        "finance_fraud.filter(col('transaction_id').isNotNull())\\\n",
        ".show()"
      ]
    },
    {
      "cell_type": "code",
      "execution_count": null,
      "metadata": {
        "id": "9HBLfyXk6Bat"
      },
      "outputs": [],
      "source": [
        "finance_fraud_processed=finance_fraud\\\n",
        ".withColumn('amount', regexp_replace('amount', '[^0-9.]', '').cast(DecimalType(12,2)))\\\n",
        ".withColumn('device_risk_score', col('device_risk_score').cast(DecimalType(12,2)))\\\n",
        ".withColumn('ip_risk_score', col('ip_risk_score').cast(DecimalType(12,2)))\\\n",
        ".withColumn('is_fraud', when(col('is_fraud') == 1,True).otherwise(False))\\\n",
        ".withColumn('transaction_type', upper(trim(col('transaction_type'))))\\\n",
        ".withColumn('merchant_category', upper(trim(col('merchant_category'))))\\\n",
        ".withColumn('country', upper(trim(col('country'))))\\\n",
        ".withColumnRenamed('hour', 'transaction_hour')\\\n",
        ".filter(col('transaction_id').isNotNull())\\\n",
        ".filter((col('transaction_hour') > 0) | (col('transaction_hour') < 23))\\\n",
        ".fillna({\n",
        "    'transaction_type': 'UNKNOWN',\n",
        "    'merchant_category': 'UNKNOWN',\n",
        "    'country': 'UNKNOWN'})\\\n",
        ".dropDuplicates(['transaction_id'])"
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "finance_fraud_processed.show(truncate=False)"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "azuwT6fm1VDJ",
        "outputId": "de3c945c-1624-4090-dbf9-20d94d8ed352"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+--------------+-------+------+----------------+-----------------+-------+----------------+-----------------+-------------+--------+\n",
            "|transaction_id|user_id|amount|transaction_type|merchant_category|country|transaction_hour|device_risk_score|ip_risk_score|is_fraud|\n",
            "+--------------+-------+------+----------------+-----------------+-------+----------------+-----------------+-------------+--------+\n",
            "|0             |21     |60.95 |ATM             |TRAVEL           |US     |17              |0.15             |0.28         |false   |\n",
            "|1             |348    |112.99|ATM             |TRAVEL           |FR     |23              |0.16             |0.20         |false   |\n",
            "|2             |725    |52.71 |QR              |TRAVEL           |US     |7               |0.08             |0.14         |false   |\n",
            "|3             |682    |113.00|ONLINE          |FOOD             |TR     |12              |0.07             |0.20         |false   |\n",
            "|4             |834    |102.66|POS             |TRAVEL           |UK     |9               |0.09             |0.04         |false   |\n",
            "|5             |565    |58.96 |ONLINE          |TRAVEL           |FR     |9               |0.29             |0.28         |false   |\n",
            "|6             |905    |177.91|ATM             |TRAVEL           |DE     |16              |0.11             |0.16         |false   |\n",
            "|7             |809    |14.72 |ATM             |TRAVEL           |DE     |9               |0.11             |0.10         |false   |\n",
            "|8             |319    |92.92 |ONLINE          |ELECTRONICS      |US     |20              |0.22             |0.00         |false   |\n",
            "|9             |125    |57.85 |ONLINE          |CLOTHING         |UK     |7               |0.05             |0.20         |false   |\n",
            "|10            |276    |191.71|QR              |ELECTRONICS      |DE     |7               |0.19             |0.29         |false   |\n",
            "|11            |975    |129.92|ATM             |TRAVEL           |DE     |14              |0.20             |0.17         |false   |\n",
            "|12            |203    |67.53 |ONLINE          |GROCERY          |FR     |18              |0.02             |0.10         |false   |\n",
            "|13            |193    |86.01 |ATM             |GROCERY          |DE     |20              |0.06             |0.13         |false   |\n",
            "|14            |680    |193.06|ONLINE          |FOOD             |TR     |12              |0.14             |0.17         |false   |\n",
            "|15            |663    |52.69 |ATM             |ELECTRONICS      |US     |22              |0.25             |0.21         |false   |\n",
            "|16            |546    |11.78 |ONLINE          |FOOD             |DE     |21              |0.22             |0.05         |false   |\n",
            "|17            |695    |114.55|ATM             |FOOD             |FR     |9               |0.26             |0.28         |false   |\n",
            "|18            |285    |115.68|POS             |GROCERY          |TR     |9               |0.26             |0.12         |false   |\n",
            "|19            |56     |133.30|QR              |ELECTRONICS      |DE     |13              |0.15             |0.19         |false   |\n",
            "+--------------+-------+------+----------------+-----------------+-------+----------------+-----------------+-------------+--------+\n",
            "only showing top 20 rows\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "finance_fraud_processed.printSchema()\n"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "PFTpFvSR1x6Y",
        "outputId": "5441fa36-7f35-4d2a-b167-12c448c6abe8"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "root\n",
            " |-- transaction_id: integer (nullable = true)\n",
            " |-- user_id: integer (nullable = true)\n",
            " |-- amount: decimal(12,2) (nullable = true)\n",
            " |-- transaction_type: string (nullable = false)\n",
            " |-- merchant_category: string (nullable = false)\n",
            " |-- country: string (nullable = false)\n",
            " |-- transaction_hour: integer (nullable = true)\n",
            " |-- device_risk_score: decimal(12,2) (nullable = true)\n",
            " |-- ip_risk_score: decimal(12,2) (nullable = true)\n",
            " |-- is_fraud: boolean (nullable = false)\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "fraud_count=finance_fraud_processed\\\n",
        ".groupby('country')\\\n",
        ".agg(count(when(col('is_fraud') == True, 1)).alias('fraud_transactions'))\\\n",
        ".orderBy('fraud_transactions', ascending=False)\\\n",
        ".show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "dQo79P1sADkC",
        "outputId": "fa20d1a2-2348-4f16-cd44-5cadaead340f"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+-------+------------------+\n",
            "|country|fraud_transactions|\n",
            "+-------+------------------+\n",
            "|     NG|               100|\n",
            "|     US|                97|\n",
            "|     UK|                85|\n",
            "|     TR|                75|\n",
            "|     FR|                74|\n",
            "|     DE|                69|\n",
            "+-------+------------------+\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "fraud_rate=finance_fraud_processed\\\n",
        ".groupBy('country') \\\n",
        ".agg(\n",
        "      (sum(col('is_fraud').cast('int')) / count('*') * 100)\n",
        "      .alias('fraud_rate_pct'))\\\n",
        ".show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "pw-n5rkSH94B",
        "outputId": "91bc60bd-49ee-4a82-f38f-57c4550b3067"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+-------+------------------+\n",
            "|country|    fraud_rate_pct|\n",
            "+-------+------------------+\n",
            "|     DE|3.5751295336787567|\n",
            "|     TR|3.8900414937759336|\n",
            "|     US|4.7317073170731705|\n",
            "|     FR|3.6507153428712384|\n",
            "|     UK| 4.325699745547073|\n",
            "|     NG|             100.0|\n",
            "+-------+------------------+\n",
            "\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "user_metrics = finance_fraud_processed\\\n",
        ".groupBy('user_id') \\\n",
        ".agg(\n",
        "        count('*').alias('total_transactions'),\n",
        "        sum(col('is_fraud').cast('int')).alias('fraud_count'),\n",
        "        sum('amount').alias('total_spent'))\\\n",
        ".show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "5MAHyCziLvBb",
        "outputId": "bf574092-c9d6-4d00-a68b-677e713470b7"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+-------+------------------+-----------+-----------+\n",
            "|user_id|total_transactions|fraud_count|total_spent|\n",
            "+-------+------------------+-----------+-----------+\n",
            "|    833|                15|          2|    1782.31|\n",
            "|    463|                15|          1|    2762.67|\n",
            "|    148|                10|          0|    1081.54|\n",
            "|    471|                 7|          0|     960.03|\n",
            "|    496|                 5|          0|     393.46|\n",
            "|    737|                13|          1|    1026.50|\n",
            "|    623|                 6|          1|    7502.35|\n",
            "|    858|                10|          0|     976.98|\n",
            "|    392|                12|          1|    1188.97|\n",
            "|    897|                12|          0|     990.23|\n",
            "|    243|                 7|          2|    2912.14|\n",
            "|    540|                13|          0|    1372.99|\n",
            "|    516|                 7|          1|     948.15|\n",
            "|     31|                 8|          1|    3538.07|\n",
            "|     85|                 8|          0|     667.99|\n",
            "|    808|                 9|          1|     914.33|\n",
            "|    451|                14|          1|    1375.78|\n",
            "|    137|                10|          0|    1217.26|\n",
            "|    251|                 7|          1|    3622.98|\n",
            "|    580|                 6|          0|     648.00|\n",
            "+-------+------------------+-----------+-----------+\n",
            "only showing top 20 rows\n"
          ]
        }
      ]
    },
    {
      "cell_type": "code",
      "source": [
        "fraud_type = finance_fraud_processed\\\n",
        ".groupBy('transaction_type') \\\n",
        ".agg(count(when(col('is_fraud') == True, 1)).alias('fraud_count'))\\\n",
        ".orderBy('fraud_count', ascending=False)\\\n",
        ".show()"
      ],
      "metadata": {
        "colab": {
          "base_uri": "https://localhost:8080/"
        },
        "id": "UM6YlvbcMhdu",
        "outputId": "1a1a174f-4143-4f80-8df7-fbedc5db6e54"
      },
      "execution_count": null,
      "outputs": [
        {
          "output_type": "stream",
          "name": "stdout",
          "text": [
            "+----------------+-----------+\n",
            "|transaction_type|fraud_count|\n",
            "+----------------+-----------+\n",
            "|             ATM|        138|\n",
            "|          ONLINE|        126|\n",
            "|              QR|        120|\n",
            "|             POS|        116|\n",
            "+----------------+-----------+\n",
            "\n"
          ]
        }
      ]
    }
  ],
  "metadata": {
    "colab": {
      "provenance": [],
      "mount_file_id": "1VprTfAsebiA5IW_nJEMhh5aoiFmdcDBr",
      "authorship_tag": "ABX9TyMvECBwxo5MQi1peXjIS5LX",
      "include_colab_link": true
    },
    "kernelspec": {
      "display_name": "Python 3",
      "name": "python3"
    },
    "language_info": {
      "name": "python"
    }
  },
  "nbformat": 4,
  "nbformat_minor": 0
}