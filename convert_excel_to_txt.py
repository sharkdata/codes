#!/usr/bin/python3
# -*- coding:utf-8 -*-

import pathlib
import pandas


class FileConverter:
    def __init__(self):
        """ """
        self.excel_files = []
        self.acc_dataframe = None

        self.default_columns = [
            "field",
            "filter",
            "public_value",
            "code",
            "swedish",
            "english",
            "synonyms",
            "ices_biology",
            "ices_physical_and_chemical",
            "bodc_nerc",
            "darwincore",
            "comments",
            "source",
        ]

        self.new_column_names = {
            "filter": "filter",
            "value": "public_value",
            "Code": "code",
            "Beskrivning/Svensk översättning": "swedish",
            "Description/English translate": "english",
            "Synonym": "synonyms",
            "ICES biology": "ices_biology",
            "ICES_biology": "ices_biology",
            "ICES physchem": "ices_physchem",
            "ICES_physical_and_chemical": "ices_physical_and_chemical",
        }

    def scan_source(self, source_dir="data_in"):
        """ """
        source_path = pathlib.Path(source_dir)
        self.excel_files = list(source_path.glob("translate_*.xlsx"))

    def convert_and_save(self, target_dir="data_out"):
        """ """
        self.acc_dataframe = pandas.DataFrame(columns=self.default_columns)

        for excel_file in sorted(self.excel_files):
            source_path = pathlib.Path(excel_file)
            print("FILE: ", source_path.name)

            df = pandas.read_excel(source_path)
            df = df.rename(columns=self.new_column_names)
            df["source"] = source_path.stem

            public_value_list = df["public_value"]
            code_list = df["code"]
            swedish_list = df["swedish"]
            english_list = df["english"]
            public_values_new = []
            for index, public_value in enumerate(public_value_list):
                if str(public_value) in ["<use_code>", "", "nan"]:
                    public_values_new.append(str(code_list[index]))
                elif str(public_value) == "<use_swedish>":
                    public_values_new.append(str(swedish_list[index]))
                elif str(public_value) == "<use_english>":
                    public_values_new.append(str(english_list[index]))
                elif str(public_value) == "<use_blank>":
                    public_values_new.append("")
                else:
                    public_values_new.append(str(public_value_list[index]))
            df["public_value"] = public_values_new

            target_dir_path = pathlib.Path(target_dir)
            target_file_path = pathlib.Path(target_dir_path, source_path.stem + ".txt")
            df.to_csv(
                target_file_path,
                sep="\t",
                index=False,
                encoding="cp1252",
                errors="replace",
            )

            # Add to concatenated list.
            self.acc_dataframe = pandas.concat([self.acc_dataframe, df])

        # Save concatenated list.
        target_file_path = pathlib.Path(target_dir, "translate_codes_NEW.txt")
        self.acc_dataframe.to_csv(
            target_file_path,
            sep="\t",
            index=False,
            encoding="cp1252",
            errors="replace",
        )


if __name__ == "__main__":
    """ """
    conv = FileConverter()
    conv.scan_source()
    conv.convert_and_save()
