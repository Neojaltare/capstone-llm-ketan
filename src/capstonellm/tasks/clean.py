import argparse
import logging
import time

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession

logger = logging.getLogger(__name__)

USER = "ketan"

# The four entities that actually occur in this corpus, plus the apostrophe.
_HTML_ENTITIES = [
    ("&quot;", '"'),
    ("&#39;", "'"),
    ("&lt;", "<"),
    ("&gt;", ">"),
    ("&amp;", "&"),
]


def load_items(spark: SparkSession, path: str) -> DataFrame:
    """Read a StackExchange API dump.

    The files are a single pretty-printed object wrapping an ``items`` array, so
    Spark needs ``multiLine`` and an explode to get one row per record.
    """
    return (
        spark.read.option("multiLine", True)
        .json(path)
        .select(F.explode("items").alias("item"))
        .select("item.*")
    )


def to_text(col: F.Column) -> F.Column:
    """Strip HTML tags and unescape entities, collapsing whitespace."""
    cleaned = F.regexp_replace(col, r"<[^>]+>", " ")
    for entity, char in _HTML_ENTITIES:
        cleaned = F.regexp_replace(cleaned, entity, char)
    return F.trim(F.regexp_replace(cleaned, r"\s+", " "))


def pick_best_answer(answers: DataFrame) -> DataFrame:
    """Reduce many answers per question down to the single best one.

    "Best" means, in order of priority:
      1. the answer the asker accepted, if there is one
      2. otherwise the highest scoring answer
      3. if two answers still tie, the one with the lower answer_id
    """
    # Step 1 -- decide the order of answers *within* each question.
    # partitionBy restarts the ordering for every question_id, so answers from
    # different questions never compete with each other.
    best_answer_first = Window.partitionBy("question_id").orderBy(
        # descending on a boolean puts True (accepted) ahead of False
        F.col("is_accepted").desc(),
        F.col("score").desc(),
        # only a tie-breaker: without it, two equally good answers could swap
        # places between runs and the output would not be reproducible
        F.col("answer_id").asc(),
    )

    # Step 2 -- number the answers of each question: 1, 2, 3, ...
    # The answer we want is always number 1.
    numbered = answers.withColumn("position", F.row_number().over(best_answer_first))

    # Step 3 -- keep only that first answer.
    winners = numbered.filter(F.col("position") == 1)

    # Step 4 -- keep just the columns the join needs. "body" is renamed because
    # questions have a "body" column too, and the join would make it ambiguous.
    return winners.select(
        F.col("question_id"),
        F.col("answer_id"),
        F.col("body").alias("answer_body"),
    )


def build_documents(questions: DataFrame, answers: DataFrame) -> DataFrame:
    """One row per answered question, matching the schema asserted in tests.

    The inner join drops questions without any answer, which is deliberate:
    ``answer_id`` is a required output field.
    """
    best = pick_best_answer(answers)
    return (
        questions.alias("q")
        .join(best.alias("a"), on="question_id", how="inner")
        .select(
            F.col("question_id"),
            to_text(F.col("q.title")).alias("title"),
            to_text(F.col("q.body")).alias("question"),
            F.col("q.link").alias("link"),
            F.col("a.answer_id").alias("answer_id"),
            to_text(F.col("a.answer_body")).alias("answer"),
        )
    )


def write_one_file_per_question(documents: DataFrame, path: str, count: int) -> None:
    """Write each document to its own file.

    ``tests/test_clean.py`` reads a whole object with ``json.loads``, so a part
    file holding several JSONL rows would fail to parse. Repartitioning to the
    row count puts exactly one document in each file.

    ``count`` is passed in rather than recomputed: the caller already knows it,
    and every ``.count()`` is another full pass over the data.
    """
    documents.repartition(max(count, 1)).write.mode("overwrite").json(path)


def clean(
    spark: SparkSession,
    environment: str,
    tag: str,
    input_dir: str = None,
    output_dir: str = None,
):
    """Build the cleaned documents for one tag.

    Defaults to reading and writing S3. Pass ``input_dir`` / ``output_dir`` to
    work against local files instead, which is the recommended way to iterate
    before touching the bucket. Both are base directories: the tag is appended,
    so ``--input ./sample_data --tag airflow`` reads ``./sample_data/airflow``.
    """
    base = f"s3a://{llm_bucket}"
    # input_dir/output_dir are *base* directories; the tag is always appended,
    # so a local run mirrors the S3 layout and several tags never collide.
    source = f"{input_dir}/{tag}" if input_dir else f"{base}/input/{tag}"
    destination = f"{output_dir}/{tag}" if output_dir else f"{base}/cleaned/{USER}/{tag}"

    started = time.monotonic()
    logger.info("[%s] reading from %s", tag, source)

    # cache() before counting: without it every .count() re-downloads and
    # re-parses the file from S3. With it, the count materialises the data once
    # and the join below reuses it, so the counts become effectively free.
    questions = load_items(spark, f"{source}/questions.json").cache()
    answers = load_items(spark, f"{source}/answers.json").cache()
    n_questions, n_answers = questions.count(), answers.count()
    logger.info("[%s] read %s questions and %s answers", tag, n_questions, n_answers)

    # cache: the count below and the write both consume this DataFrame, and
    # without caching Spark would recompute the whole join for each.
    documents = build_documents(questions, answers).cache()
    n_documents = documents.count()
    logger.info(
        "[%s] %s documents to write (%s questions dropped: no answer)",
        tag, n_documents, n_questions - n_documents,
    )

    logger.info("[%s] writing to %s ...", tag, destination)
    write_one_file_per_question(documents, destination, n_documents)
    for cached in (documents, questions, answers):
        cached.unpersist()

    logger.info(
        "[%s] DONE - wrote %s documents in %.1fs",
        tag, n_documents, time.monotonic() - started,
    )


def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tags", nargs="+", metavar="TAG",
        help="one or more tags to process, e.g. --tag airflow dbt sql",
        default=["python-polars"], required=False
    )
    parser.add_argument(
        "-i", "--input", dest="input_dir", required=False, default=None,
        help="local directory holding questions.json/answers.json (default: read from S3)",
    )
    parser.add_argument(
        "-o", "--output", dest="output_dir", required=False, default=None,
        help="local directory to write the cleaned documents to (default: write to S3)",
    )
    args = parser.parse_args()

    # Without this, every logger.info() in this module is silently discarded:
    # the root logger defaults to WARNING and has no handler attached.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
        datefmt="%H:%M:%S",
    )
    logger.info("starting the cleaning job for %s tag(s): %s", len(args.tags), ", ".join(args.tags))
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider",
        # Commit algorithm v1 renames every output file one at a time, and a
        # "rename" on S3 is really a copy plus a delete. v2 commits each task
        # directly, which roughly halves the S3 calls when writing one file per
        # question.
        "spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version": "2",
        # More parallel connections: this job writes many small objects, so it
        # is latency-bound rather than bandwidth-bound.
        "spark.hadoop.fs.s3a.connection.maximum": "100",
        "spark.hadoop.fs.s3a.fast.upload": "true",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        builder = SparkSession.builder.appName("Spark S3 Integration").config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.4.2"
        )
        for key, value in common_spark_config.items():
            builder = builder.config(key, value)
        session = builder.getOrCreate()
        for position, tag in enumerate(args.tags, start=1):
            logger.warning("=== tag %s of %s: %s ===", position, len(args.tags), tag)
            clean(session, args.env, tag, args.input_dir, args.output_dir)

    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            for position, tag in enumerate(args.tags, start=1):
                logger.info("=== tag %s of %s: %s ===", position, len(args.tags), tag)
                clean(session, args.env, tag, args.input_dir, args.output_dir)


if __name__ == "__main__":
    main()
