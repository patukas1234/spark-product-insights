spark-submit \
  --master yarn \
  --deploy-mode client \
  --class ds.insights.AmazonProductsAnalytics \
  --files ./src/main/resources/application.conf \
  --driver-java-options -Dconfig.file=./src/main/resources/application.conf \
  --conf spark.driver.extraJavaOptions=-Dconfig.file=./src/main/resources/application.conf \
  --conf spark.executor.extraJavaOptions=-Dconfig.file=./src/main/resources/application.conf \
  ./target/scala-2.12/sbt-1.0/spark_product_insights-assembly-0.7.0-SNAPSHOT.jar