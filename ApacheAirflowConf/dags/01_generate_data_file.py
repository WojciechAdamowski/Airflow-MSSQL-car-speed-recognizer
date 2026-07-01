from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.filesystem import FileSensor
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk import chain
from datetime import datetime, timedelta

import random
import string


def generate_data_file(**context):
    import pandas as pd

    random.seed(42)

    def generate_polish_plate():
        prefixes = [
            "WA", "WB", "WD", "WE", "WF", "WH", "WI", "WK", "WN", "WT",
            "KR", "KK", "KN", "PO", "PY", "PL", "DW", "GD", "GA", "LU",
            "RZ", "BI", "EL", "CB", "FG", "NO", "SZ", "ZG", "OP", "PK"
        ]
        prefix = random.choice(prefixes)

        patterns = [
            lambda p: p + ''.join(random.choices(string.digits, k=5)),
            lambda p: p + ''.join(random.choices(string.digits, k=4)) + random.choice(string.ascii_uppercase),
            lambda p: p + ''.join(random.choices(string.ascii_uppercase, k=1)) + ''.join(
                random.choices(string.digits, k=4)),
            lambda p: p + ''.join(random.choices(string.ascii_uppercase + string.digits, k=5)),
        ]
        return random.choice(patterns)(prefix)

    def mutate_plate(plate):
        if not plate:
            return plate

        error_type = random.choice([
            "missing_last_char",
            "extra_space",
            "lowercase",
            "char_swap",
            "empty",
            "null_like"
        ])

        if error_type == "missing_last_char" and len(plate) > 1:
            return plate[:-1]
        if error_type == "extra_space":
            pos = random.randint(1, len(plate) - 1)
            return plate[:pos] + " " + plate[pos:]
        if error_type == "lowercase":
            return plate.lower()
        if error_type == "char_swap":
            swaps = {"0": "O", "O": "0", "1": "I", "I": "1", "2": "Z", "Z": "2", "5": "S", "S": "5", "8": "B", "B": "8"}
            positions = [i for i, ch in enumerate(plate) if ch in swaps]
            if positions:
                idx = random.choice(positions)
                return plate[:idx] + swaps[plate[idx]] + plate[idx + 1:]
            return plate
        if error_type == "empty":
            return ""
        if error_type == "null_like":
            return None

        return plate

    def generate_segment_catalog():
        return [
            {"segment_id": 101, "segment_name": "S8 Radzymin - Wyszków", "segment_length_m": 8200,
             "speed_limit_kmh": 120},
            {"segment_id": 102, "segment_name": "A2 Mińsk Mazowiecki - Kałuszyn", "segment_length_m": 14600,
             "speed_limit_kmh": 140},
            {"segment_id": 103, "segment_name": "DK7 Płońsk - Glinojeck", "segment_length_m": 5400,
             "speed_limit_kmh": 90},
            {"segment_id": 104, "segment_name": "S7 Mława - Strzegowo", "segment_length_m": 11300,
             "speed_limit_kmh": 120},
            {"segment_id": 105, "segment_name": "A1 Tuszyn - Piotrków Tryb.", "segment_length_m": 15800,
             "speed_limit_kmh": 140},
            {"segment_id": 106, "segment_name": "DK8 Kaczorów - Bolków", "segment_length_m": 3900,
             "speed_limit_kmh": 70},
            {"segment_id": 107, "segment_name": "S3 Polkowice - Lubin", "segment_length_m": 9700,
             "speed_limit_kmh": 100},
            {"segment_id": 108, "segment_name": "DK92 Sochaczew - Ożarów", "segment_length_m": 6100,
             "speed_limit_kmh": 50},
            {"segment_id": 109, "segment_name": "A4 Opole - Brzeg", "segment_length_m": 12500, "speed_limit_kmh": 140},
            {"segment_id": 110, "segment_name": "S17 Garwolin - Ryki", "segment_length_m": 8800,
             "speed_limit_kmh": 120},
            {"segment_id": 111, "segment_name": "DK91 Toruń - Chełmno", "segment_length_m": 4700,
             "speed_limit_kmh": 90},
            {"segment_id": 112, "segment_name": "S19 Lublin - Kraśnik", "segment_length_m": 10200,
             "speed_limit_kmh": 100},
        ]

    def weighted_vehicle_type():
        vehicle_types = [
            ("OSOBOWY", 70),
            ("DOSTAWCZY", 12),
            ("CIĘŻAROWY", 10),
            ("MOTOCYKL", 4),
            ("AUTOBUS", 2),
            ("INNY", 2),
        ]
        values = [v[0] for v in vehicle_types]
        weights = [v[1] for v in vehicle_types]
        return random.choices(values, weights=weights, k=1)[0]

    def estimate_travel_time_seconds(segment_length_m, speed_limit_kmh, vehicle_type):
        base_speed_factor = {
            "OSOBOWY": random.uniform(0.75, 1.10),
            "DOSTAWCZY": random.uniform(0.70, 1.00),
            "CIĘŻAROWY": random.uniform(0.60, 0.90),
            "MOTOCYKL": random.uniform(0.80, 1.15),
            "AUTOBUS": random.uniform(0.60, 0.90),
            "INNY": random.uniform(0.60, 1.00),
        }

        speed = max(20, speed_limit_kmh * base_speed_factor[vehicle_type])
        seconds = segment_length_m / (speed * 1000 / 3600)

        traffic_factor = random.uniform(0.95, 1.35)
        return int(seconds * traffic_factor)

    def inject_record_errors(record, plate_error_rate=0.04, time_error_rate=0.05, segment_error_rate=0.02,
                             text_error_rate=0.02):
        r = record.copy()

        if random.random() < plate_error_rate:
            r["plate_number"] = mutate_plate(r["plate_number"])

        if random.random() < time_error_rate:
            time_error = random.choice([
                "missing_entry",
                "missing_exit",
                "exit_before_entry",
                "same_timestamp",
                "very_long_trip"
            ])
            if time_error == "missing_entry":
                r["entry_timestamp"] = None
            elif time_error == "missing_exit":
                r["exit_timestamp"] = None
            elif time_error == "exit_before_entry" and r["entry_timestamp"] is not None:
                r["exit_timestamp"] = r["entry_timestamp"] - timedelta(seconds=random.randint(10, 600))
            elif time_error == "same_timestamp" and r["entry_timestamp"] is not None:
                r["exit_timestamp"] = r["entry_timestamp"]
            elif time_error == "very_long_trip" and r["entry_timestamp"] is not None:
                r["exit_timestamp"] = r["entry_timestamp"] + timedelta(hours=random.randint(3, 8))

        if random.random() < segment_error_rate:
            segment_error = random.choice([
                "negative_length",
                "zero_length",
                "missing_limit",
                "unknown_segment_id"
            ])
            if segment_error == "negative_length":
                r["segment_length_m"] = -abs(r["segment_length_m"])
            elif segment_error == "zero_length":
                r["segment_length_m"] = 0
            elif segment_error == "missing_limit":
                r["speed_limit_kmh"] = None
            elif segment_error == "unknown_segment_id":
                r["segment_id"] = 999999

        if random.random() < text_error_rate:
            text_error = random.choice([
                "segment_name_typo",
                "vehicle_type_lower",
                "vehicle_type_unknown"
            ])
            if text_error == "segment_name_typo":
                r["segment_name"] = r["segment_name"].replace("-", "").replace("ó", "o")
            elif text_error == "vehicle_type_lower":
                r["vehicle_type"] = r["vehicle_type"].lower()
            elif text_error == "vehicle_type_unknown":
                r["vehicle_type"] = "NIEZNANY"

        return r

    def generate_speed_segment_data(
            n_records=10000,
            start_date=datetime(2024, 1, 1, 0, 0, 0),
            days=30
    ):
        segments = generate_segment_catalog()

        unique_plates = [generate_polish_plate() for _ in range(max(3000, int(n_records * 0.55)))]

        records = []

        for _ in range(n_records):
            segment = random.choice(segments)
            plate = random.choice(unique_plates)
            vehicle_type = weighted_vehicle_type()

            random_seconds = random.randint(0, days * 24 * 3600 - 1)
            entry_timestamp = start_date + timedelta(seconds=random_seconds)

            travel_time_seconds = estimate_travel_time_seconds(
                segment_length_m=segment["segment_length_m"],
                speed_limit_kmh=segment["speed_limit_kmh"],
                vehicle_type=vehicle_type
            )

            exit_timestamp = entry_timestamp + timedelta(seconds=travel_time_seconds)

            record = {
                "entry_timestamp": entry_timestamp,
                "exit_timestamp": exit_timestamp,
                "segment_id": segment["segment_id"],
                "segment_name": segment["segment_name"],
                "segment_length_m": segment["segment_length_m"],
                "speed_limit_kmh": segment["speed_limit_kmh"],
                "plate_number": plate,
                "vehicle_type": vehicle_type
            }

            record = inject_record_errors(record)
            records.append(record)

        df = pd.DataFrame(records, columns=[
            "entry_timestamp",
            "exit_timestamp",
            "segment_id",
            "segment_name",
            "segment_length_m",
            "speed_limit_kmh",
            "plate_number",
            "vehicle_type"
        ])

        return df

    file_path = BaseHook.get_connection("source_fs").extra_dejson["path"]
    date_time_now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    row_counts = context["params"]["row_counts"]
    df = generate_speed_segment_data(n_records=row_counts)
    df.to_csv(file_path + f"/car_speed_data_{date_time_now}.csv", index=False)


with DAG(dag_id="01_generate_data_file", start_date=None, schedule=None, tags={"generate_data"}, params={"row_counts": 10000}):
    t_generate_data_file = PythonOperator(
        task_id="generate_data_file"
        , python_callable=generate_data_file
    )



