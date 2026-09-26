import random
from datetime import datetime, timezone

from producers.event_sequence import generate_patient_events
from producers.kafka_producer import create_producer
from producers.patient_generator import generate_patients


PATIENT_COUNT = 20
EVENTS_PER_PATIENT = 10
SEED = 42


def main():
    rng = random.Random(SEED)

    patients = generate_patients(
        count=PATIENT_COUNT,
        seed=SEED,
    )

    producer = create_producer()

    start_ts = datetime.now(timezone.utc)

    total_events = 0

    try:
        for patient in patients:
            events = generate_patient_events(
                patient_id=patient["patient_id"],
                baseline_hr=patient["baseline_hr"],
                baseline_spo2=patient["baseline_spo2"],
                start_ts=start_ts,
                count=EVENTS_PER_PATIENT,
                rng=rng,
            )

            for event in events:
                future = producer.send(
                    "vitals.events",
                    key=event["patient_id"].encode("utf-8"),
                    value=event,
                )

                future.get(timeout=10)
                total_events += 1

        producer.flush()

        print(f"Published {total_events} events")

    finally:
        producer.close()


if __name__ == "__main__":
    main()