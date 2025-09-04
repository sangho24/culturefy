from pyspark.sql import SparkSession
# Import the necessary types to define a schema
from pyspark.sql.types import StructType, StructField, StringType

# 1. Spark 세션 시작
spark = SparkSession.builder.appName("K-Culturefy-Processing").master("local[*]").getOrCreate()

print("Spark 세션이 성공적으로 생성되었습니다.")

# 2. 데이터의 스키마(구조)를 직접 정의
# 그냥 StringType()으로 하면 null 값이 들어올 수도 있으니 True로 설정
# Our JSON files have 'id', 'url', 'title', and 'text', and they are all strings.
schema = StructType([
    StructField("id", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("text", StringType(), True)
])

# 3. 'extracted_text' 폴더의 모든 JSON 파일을 "정의된 스키마"를 사용해 읽어오기
extracted_data_path = "/Users/sangho/k-culturefy-project/extracted_text/*/*"
# Add the .schema(schema) option here
wiki_df = spark.read.schema(schema).json(extracted_data_path)

print("데이터를 성공적으로 불러왔습니다.")

# 4. 데이터 확인
print("데이터 스키마:")
wiki_df.printSchema()
print("데이터 샘플:")
wiki_df.show(5)

total_count = wiki_df.count()
print(f"총 {total_count}개의 문서를 불러왔습니다.")

# 5. Spark 세션 종료
spark.stop()