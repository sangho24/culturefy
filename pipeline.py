# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession
# explode, pandas_udf 등 새로운 함수들을 불러옵니다.
from pyspark.sql.functions import col, length, regexp_replace, udf, explode, pandas_udf
from pyspark.sql.types import ArrayType, StringType, StructType, StructField, FloatType
import pandas as pd

def main():
    """
    Main function to run the data processing pipeline.
    """
    # pipeline.py
    # 로컬에서 4개의 코어를 사용하여 Spark 세션을 생성합니다.
    spark = SparkSession.builder \
        .appName("K-Culturefy-Pipeline") \
        .master("local[4]") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    print("Spark 세션이 성공적으로 생성되었습니다.")
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("url", StringType(), True),
        StructField("title", StringType(), True),
        StructField("text", StringType(), True)
    ])

    # --- 👇 [수정됨] 경로를 컨테이너 내부 경로로 변경 ---
    wiki_df = spark.read.schema(schema).json("/app/extracted_text/*/*")
    
    print("데이터 정제를 시작합니다...")
    cleaned_df = wiki_df.filter(
        (col("text").isNotNull()) & (length(col("text")) > 100)
    ).withColumn("text", regexp_replace(col("text"), "[\n\r]", " "))

    cleaned_df.cache()
    cleaned_count = cleaned_df.count()
    print(f"정제 후 남은 문서 개수: {cleaned_count}")

    print("\n의미 기반 분할(Chunking)을 시작합니다...")

    def semantic_chunking(text):
        paragraphs = text.split('\n\n')
        chunks = []
        current_chunk = ""
        chunk_size = 1000

        for p in paragraphs:
            trimmed_p = p.strip()
            if not trimmed_p: continue
            if len(current_chunk) + len(trimmed_p) + 2 > chunk_size and current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            current_chunk = f"{current_chunk}\n\n{trimmed_p}" if current_chunk else trimmed_p
        if current_chunk: chunks.append(current_chunk)
        return chunks

    chunking_udf = udf(semantic_chunking, ArrayType(StringType()))

    chunked_df = cleaned_df.withColumn("chunks", chunking_udf(cleaned_df["text"]))

    # --- 👇 [수정됨] 텍스트 조각들을 개별 행으로 분리 ---
    # ['chunk1', 'chunk2'] -> (row1, chunk1), (row1, chunk2)
    exploded_df = chunked_df.withColumn("chunk", explode(col("chunks"))) \
                            .select("id", "title", "url", "chunk")

    print("\n벡터 변환을 시작합니다. 이 과정은 시간이 오래 걸릴 수 있습니다...")
    
    # --- 👇 [수정됨] Pandas UDF를 사용한 벡터 변환 ---
    # Pandas UDF 함수 정의
    @pandas_udf(ArrayType(FloatType()))
    def embed_texts(texts: pd.Series) -> pd.Series:
        from sentence_transformers import SentenceTransformer
        # 모델은 각 일꾼(worker)마다 한 번만 로드됩니다.
        model = SentenceTransformer('jhgan/ko-sroberta-multitask')
        # to_list()로 변환하여 모델에 전달
        embeddings = model.encode(texts.to_list(), show_progress_bar=False)
        # 결과를 다시 pandas Series로 반환
        return pd.Series(list(embeddings))

    # Pandas UDF를 적용하여 'embedding' 컬럼 생성
    embedded_df = exploded_df.withColumn("embedding", embed_texts(col("chunk")))

    # --- 👇 [수정됨] ChromaDB 저장을 위한 foreachPartition ---
    print("\nChromaDB 저장을 시작합니다...")

    def save_to_chromadb(iterator):
        import chromadb
        db_client = chromadb.PersistentClient(path="/app/korea_db")
        collection = db_client.get_or_create_collection("korea_db")
        
        rows = list(iterator)
        if not rows: return

        ids = [f"{row.id}_{i}" for i, row in enumerate(rows)]
        embeddings = [row.embedding for row in rows]
        documents = [row.chunk for row in rows]
        metadatas = [{'title': row.title, 'url': row.url} for row in rows]
        
        collection.add(ids=ids, embeddings=embeddings, documents=documents, metadatas=metadatas)

    # 파티션을 나눠서 저장
    embedded_df.repartition(16).foreachPartition(save_to_chromadb)

    print("\n모든 작업이 완료되었습니다!")
    print("프로젝트 폴더 안에 'korea_db' 폴더가 생성되었습니다.")

    spark.stop()

if __name__ == "__main__":
    main()

