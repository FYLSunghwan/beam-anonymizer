import argparse
from typing import List

import apache_beam as beam
from apache_beam.dataframe.convert import to_pcollection
from apache_beam.dataframe.io import read_csv
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from beam_anonymizer.ops import GetKAnonymity
from beam_anonymizer.ops.anonymizer import Grouping, Masking, Rounding

# fmt: off
parser = argparse.ArgumentParser()

group = parser.add_argument_group(title="Pipeline config", description="파이프라인의 입력, 출력, 버젼 등 실행의 핵심적인 정보를 설정합니다.")
group.add_argument("-i", "--input-csv-path", type=str, required=True, help="Input CSV의 위치입니다.")
group.add_argument("-o", "--output-name-prefix", type=str, required=True, help="Output CSV의 이름 Prefix입니다.")
group.add_argument("-r", "--report-name-prefix", type=str, required=True, help="Report 파일의 이름 Prefix입니다.")

group = parser.add_argument_group(title="Beam options", description="Apache Beam의 PipelineOptions에 덮어씌워지는 옵션들입니다.")
group.add_argument("--runner", type=str, default="DirectRunner", help="Apache Beam의 Runner입니다.")
group.add_argument("--direct_running_mode", type=str, default="multi_threading", help="Direct Runner default running mode")
group.add_argument("--direct_num_workers", type=int, default=0, help="Direct Runner default number of workers")
# fmt: on


def main(args: argparse.Namespace, pipeline_args: List[str], save_main_session: bool = True):

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        """
        Step 1. Data를 CSV로부터 불러옵니다.
        """
        dataframe = p | "ReadCSV" >> read_csv(args.input_csv_path, usecols=lambda x: x.replace(" ", "_"))
        dataframe = dataframe.rename(columns=lambda x: x.replace(" ", "_"))
        input_pcoll = to_pcollection(dataframe)

        """
        Step 2. 데이터를 익명화합니다.
        """
        anonymized = (
            input_pcoll
            | "Grouping"
            >> beam.ParDo(
                Grouping(
                    ["만나이"],
                    boundaries=[13, 16, 19, 22, 26, 29, 39],
                    group_names=["-13", "14-16", "17-19", "20-22", "23-26", "27-29", "30-39", "40-"],
                )
            )
            | "Masking" >> beam.ParDo(Masking(["이름"], max_mask_len=2, align_left=False))
            | "Rounding" >> beam.ParDo(Rounding(["월_근로소득"], 10000))
        )

        """
        Step 3. K-익명성을 구합니다.
        """
        k_anonymity = anonymized | GetKAnonymity(["이름", "성별"])

        """
        Step 4. 결과를 저장합니다.
        """
        header_items = [item.replace("_", " ") for item in list(dataframe.keys())]
        header = ",".join(header_items)
        (
            anonymized
            | "DumpCSV" >> beam.Map(lambda x: ",".join([str(getattr(x, key, "")) for key in header_items]))
            | "WriteCSV" >> beam.io.WriteToText(args.output_name_prefix, header=header, file_name_suffix=".csv")
        )
        k_anonymity["top_duplicates"] | "WriteKReport" >> beam.io.WriteToText(
            args.report_name_prefix + "_k", file_name_suffix=".txt"
        )
        k_anonymity["distribution"] | "WriteKDistribution" >> beam.io.WriteToText(
            args.report_name_prefix + "_kdist", file_name_suffix=".txt"
        )


if __name__ == "__main__":
    known_args, pipeline_args = parser.parse_known_args()

    # fmt: off
    pipeline_args.extend(
        [
            f"--runner={known_args.runner}",
            f"--direct_running_mode={known_args.direct_running_mode}",
            f"--direct_num_workers={known_args.direct_num_workers}",
        ]
    )
    # fmt: on
    main(args=known_args, pipeline_args=pipeline_args)
