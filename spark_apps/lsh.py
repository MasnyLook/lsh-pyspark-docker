import hashlib
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import random
import re

def preprocess(question):
    question = question.lower() 
    question = re.sub(r'[^a-zA-Z0-9 ]+', '', question)
    return question 

def make_shingles(question, k):
    return [question[i:i+k] for i in range(len(question)-k+1)] if len(question) >= k else []

def create_signature(shingles, broadcast_seeds):
    signature = []
    for seed in broadcast_seeds:
        min_hash = min(
            int(hashlib.md5(f"{s}{seed}".encode()).hexdigest()[:8], 16) for s in shingles
        ) if shingles else 0
        signature.append(min_hash)
    return signature

def band_hash(band):
    return int(hashlib.md5("".join(map(str, band)).encode()).hexdigest()[:8], 16)


def main():
    shingle_size = 4
    signature_size = 50
    num_bands = 10

    conf=SparkConf()
    conf.set("spark.executor.memory", "6g")
    conf.set("spark.driver.memory", "6g")
    conf.setAppName(f"LSH analyzer, shingle_size: {shingle_size}, signature_size: {signature_size}, num_bands: {num_bands}")

    sc = SparkContext.getOrCreate(conf)

    questions_path = "/opt/spark/data/assignment_questions.csv"
    gold_sample_path = "/opt/spark/data/assignment_gold_sample.csv"

    # Przetwarzamy pytania do RDD, usuwamy znaki specjalne, postać {id: question}
    questions_rdd = sc.textFile(questions_path) \
        .map(lambda line: line.split(",")) \
        .filter(lambda x: len(x) > 1 and x[0] != "id" and x[0].isdigit()) \
        .map(lambda x: (int(x[0]), preprocess(x[1].strip())))

    # Przetwarzamy znane pary, jako klucz dajemy narazie id pierwszego pytania, postać {id: (id2, value)}
    gold_pairs = sc.textFile(gold_sample_path) \
        .map(lambda line: line.split(",")) \
        .filter(lambda x: len(x) == 3 and x[0] != "qid1") \
        .map(lambda x: (int(x[0]), (int(x[1]), int(x[2])))) \
        .distinct()

    # Tworzymy RDD z shingles, postać {question_id: [shingle1, shingle2, ...]}
    shingles_rdd = questions_rdd.mapValues(lambda question: make_shingles(question, shingle_size)).persist()

    # Tworzymy seedy dla funkcji haszujących shignle
    hash_seeds = [random.randint(0, 2**32) for _ in range(signature_size)]
    # broadcast_seeds = sc.broadcast(hash_seeds)
    broadcast_seeds = [random.randint(0, 2**32) for _ in range(signature_size)]
    # broadcast_seeds = [3646798515, 20632853, 1924355997, 2813258669, 897037277, 1510520485, 2835793175, 3562787004, 4260788798, 1796992687, 1791768213, 3570753509, 1055406813, 3902062270, 2835710828, 1279227446, 2285974268, 1956597397, 276158360, 3485137740, 3646967364, 1691485754, 230406902, 2604928405, 1419501399, 1364430976, 4072675881, 1531760221, 3093534820, 734903156, 4170989928, 2700074238, 1818251861, 4119385983, 1802736899, 2095298345, 3683688909, 487799583, 4022097722, 2750551365, 2849916662, 3971404097, 3011318716, 854542203, 2576754756, 3083350607, 207870587, 610878916, 2107821498, 2008447528, 3797388966, 1961498714, 2641801248, 1393086400, 1541907735, 2951538026, 2779947522, 2680463119, 2182535771, 1533102354, 2211221137, 2460094005, 2759591294, 3693381718, 2098866793, 1823305630, 1965360225, 177496865, 733523201, 1855870932, 2142600284, 479810803, 3819329244, 665871027, 2068008802, 1277055779, 525341537, 2479431193, 3705525580, 4028255913, 2102928633, 440015185, 2066294723, 64909738, 3659910647, 2670110098, 2431412670, 656419802, 1119560550, 561067390, 267800057, 3550530617, 3966344859, 3925231226, 2384275550, 1568452012, 349063069, 713648294, 1682400860, 1550857543]

    # Tworzymy sygnatury, postać {question_id: [signature1, signature2, ...]}
    signatures_rdd = shingles_rdd.mapValues(lambda shingles: create_signature(shingles, broadcast_seeds)).persist()

    rows_per_band = signature_size // num_bands

    # Tworzymy RDD z bandami, postać {question_id: [band1, band2, ...]}
    bands_rdd = signatures_rdd.map(
        lambda x: (
            x[0],
            [band_hash(x[1][i * rows_per_band:(i + 1) * rows_per_band]) for i in range(num_bands)]
        )
    ).persist()

    # Tworzymy RDD z parami, gdzie dołącamy bandy do id pierwszego pytania 
    # postać : {question_id: ((id2, value), [band1, band2, ...])}
    linked_1_gold_pairs = gold_pairs.join(bands_rdd)

    # Mapujemy na postać {id2: ([band1, band2, ...], value)}
    mapped_linked_1_gold_pairs_rdd = linked_1_gold_pairs.map(
        lambda x: (
            x[1][0][0],
            (x[1][1], x[1][0][1])
        )
    )

    # Dołączamy bandy do id drugiego pytania
    # postać : {question_id_2: (([band1, band2, ...], value), [band1, band2, ...])}
    linked_gold_pairs = mapped_linked_1_gold_pairs_rdd.join(bands_rdd)

    # Sprawdzany czy dane pary pytań są potencjalnie takie same
    # Następnie wybieramy tylko wartości 1 i 0 z nich
    filtered_rdd = linked_gold_pairs.filter(
        lambda x: any(
            x[1][1][i] == x[1][0][0][i] for i in range(len(x[1][1]))
        )
    ).map(lambda x: x[1][0][1])

    count_ones = filtered_rdd.filter(lambda x: x == 1).count()
    count_zeros = filtered_rdd.filter(lambda x: x == 0).count()

    print(f"Liczba jedynek (TP): {count_ones}")
    print(f"Liczba zer (FP): {count_zeros}")

    # Zapisujemy wyniki do pliku
    with open("/opt/spark/data/results.txt", "a") as file:
        file.write(f"params: shingle_size: {shingle_size}, signature_size: {signature_size}, num_bands: {num_bands}\n")
        file.write(f"Liczba jedynek (TP): {count_ones}\n")
        file.write(f"Liczba zer (FP): {count_zeros}\n")

    bands_rdd.unpersist()
    signatures_rdd.unpersist()
    shingles_rdd.unpersist()

main()

