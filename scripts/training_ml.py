# Ejemplo simple dentro del shell PySpark
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

# Supongamos que quieres predecir 'amount' basado en 'balance_after_transaction'
# Primero, convierte las columnas numéricas relevantes en un vector de features
assembler = VectorAssembler(inputCols=["balance_after_transaction"], outputCol="features")
data_with_features = assembler.transform(df)

# Divide los datos en conjuntos de entrenamiento y prueba
(trainingData, testData) = data_with_features.randomSplit([0.7, 0.3], seed=123)

# Crea un modelo de regresión lineal
lr = LinearRegression(featuresCol="features", labelCol="amount")

# Entrena el modelo
lr_model = lr.fit(trainingData)

# Haz predicciones en los datos de prueba
predictions = lr_model.transform(testData)
predictions.select("amount", "prediction", "features").show(5)

# Evalúa el modelo
evaluator = RegressionEvaluator(labelCol="amount", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

# Puedes guardar el modelo para usarlo después
# lr_model.save("hdfs:///user/ml_models/linear_regression_bank_transactions")