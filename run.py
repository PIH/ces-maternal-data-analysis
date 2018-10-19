#! ./env/bin/python3

import argparse
from pathlib import Path
from pprint import pprint

import ezcsv
from fuzzywuzzy import fuzz, process
from dateutil import parser


# Constants ###########################################################################

INPUT_DIR = Path(".") / "input"
CENSO_FILENAME = "SM_Database2_Cleaned.csv"
CENSO_PATH = INPUT_DIR / CENSO_FILENAME
PARTOS_FILENAME = "partos-clean.csv"
PARTOS_PATH = INPUT_DIR / PARTOS_FILENAME
REFS_FILENAME = "REFERENCIAS 2018.csv"
REFS_PATH = INPUT_DIR / PARTOS_FILENAME

OUTPUT_DIR = Path(".") / "output"
PARTO_MATCHES_FILENAME = "parto-matches.csv"
PARTO_MATCHES_PATH = OUTPUT_DIR / PARTO_MATCHES_FILENAME

# Parameters ##########################################################################

DATES_ARE_DAY_FIRST = True
NAME_MATCH_THRESHOLD = 90


# Main ################################################################################


def get_data():
    raw_censo_data = ezcsv.read_dicts(CENSO_PATH)
    raw_partos_data = ezcsv.read_dicts(PARTOS_PATH)
    raw_refs_data = ezcsv.read_dicts(REFS_PATH)
    return (raw_censo_data, raw_partos_data, raw_refs_data)


def main():
    (raw_censo_data, raw_partos_data, raw_refs_data) = get_data()

    partos_names = [raw_parto_line["NOMBRE "] for raw_parto_line in raw_partos_data]

    deliveries_by_parto_name = {}
    for parto_line in raw_partos_data:
        name = parto_line["NOMBRE "]
        if name not in deliveries_by_parto_name:
            deliveries_by_parto_name[name] = []
        deliveries_by_parto_name[name].append(parto_line)

    matches = []
    for censo_line in raw_censo_data:
        censo_name = censo_line["Prgnc_Paciente"]

        (parto_name, name_score) = process.extractOne(
            censo_name, partos_names, scorer=fuzz.token_sort_ratio
        )
        parto_deliveries = deliveries_by_parto_name[parto_name]

        # Check gestational age
        for parto_delivery in parto_deliveries:
            ga_match = check_ga_match(censo_line, parto_delivery)

            if ga_match and name_score > NAME_MATCH_THRESHOLD:
                result_line = {
                    **{"CENSO-" + k: v for k, v in censo_line.items()},
                    **{"PARTO-" + k: v for k, v in parto_delivery.items()},
                }
                matches.append(result_line)
                print(censo_name, parto_name, name_score)

    ezcsv.write_dicts(matches, PARTO_MATCHES_PATH, mkdir=True)


def check_ga_match(censo_line, parto_line):
    try:
        raw_censo_fum = censo_line["Prgnc_Fecha_de_ultima_menstruacion"]
        censo_fum = parser.parse(raw_censo_fum, dayfirst=DATES_ARE_DAY_FIRST)
        raw_parto_fp = parto_line["FECHA Y HORA DE NACIMIENTO"]
        parto_fp = parser.parse(raw_parto_fp, dayfirst=DATES_ARE_DAY_FIRST)
        ga = parto_fp - censo_fum
        # print(raw_censo_fum, raw_parto_fp, ga.days)
        return 365 > ga.days > 0
    except ValueError:
        return True

    # age_score = check_age_match(censo_line, parto_match)
    # gesta_score = check_gesta_match(censo_line, parto_match)


def validate_manual_matches():
    (raw_censo_data, raw_partos_data, raw_refs_data) = get_data()

    censo_has_match = [
        (l["CAMATID"], l["Prgnc_Paciente"])
        for l in raw_censo_data
        if l["CAMATID"].strip() != ""
    ]

    parto_by_camatid = {p["CAMATID"]: p for p in raw_partos_data}
    parto_manual_matches = []
    for (cid, cname) in censo_has_match:
        try:
            cid_int = cid.replace("CAMAT-", "")
            _ = int(cid_int)
            parto_manual_match = (cid_int, parto_by_camatid[cid_int]["NOMBRE "])
            parto_manual_matches.append(parto_manual_match)
            name_score = fuzz.token_sort_ratio(
                cname.lower().strip(), parto_manual_match[1]
            )
            print(
                parto_manual_match[0],
                "\t",
                cname,
                "\t",
                parto_manual_match[1],
                ":\t",
                name_score,
            )
        except ValueError:
            print("Couldn't find match for {}: {}".format(cid, cname))


if __name__ == "__main__":
    main()
